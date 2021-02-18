# python3
# coding=utf-8
# Copyright 2020 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Custom hook to monitor and log TCRM info into BigQuery."""

import datetime
import enum
import json
from typing import Any, Dict, List, Tuple, Generator

from airflow import exceptions
from airflow.contrib.hooks import bigquery_hook

from gps_building_blocks.tcrm.hooks import input_hook_interface
from gps_building_blocks.tcrm.utils import blob
from gps_building_blocks.tcrm.utils import errors
from gps_building_blocks.tcrm.utils import retry_utils


class MonitoringEntityMap(enum.Enum):
  RUN = -1
  BLOB = -2
  REPORT = -3
  RETRY = -4

_DEFAULT_PAGE_SIZE = 1000

_BASE_BQ_HOOK_PARAMS = ('delegate_to', 'use_legacy_sql', 'location')

_LOG_SCHEMA_FIELDS = [
    {'name': 'dag_name', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
    {'name': 'type_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    {'name': 'location', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'position', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'info', 'type': 'STRING', 'mode': 'NULLABLE'}]


def _generate_zone_aware_timestamp() -> str:
  """Returns the current timezone aware timestamp."""
  return datetime.datetime.utcnow().isoformat() + 'Z'


class MonitoringHook(
    bigquery_hook.BigQueryHook, input_hook_interface.InputHookInterface):
  """Custom hook monitoring TCRM.

  Attributes:
    dataset_id: Unique name of the dataset.
    table_id: Unique location within the dataset.
  """

  def __init__(self,
               bq_conn_id: str,
               monitoring_dataset: str,
               monitoring_table: str,
               dag_name: str = '',
               location: str = '',
               enable_monitoring: bool = True,
               **kwargs) -> None:
    """Initializes the generator of a specified BigQuery table.

    Args:
      bq_conn_id: Connection id passed to airflow's BigQueryHook.
      monitoring_dataset: Dataset id of the monitoring table.
      monitoring_table: Table name of the monitoring table.
      dag_name: Airflow DAG ID that is associated with the current monitoring.
      location: The input resource location URL for the current run.
      enable_monitoring: A retry entity will be logged in monitoring table if
          True.
      **kwargs: Other arguments to pass through to Airflow's BigQueryHook.
    """
    init_params_dict = {}
    for param in _BASE_BQ_HOOK_PARAMS:
      if param in kwargs:
        init_params_dict[param] = kwargs[param]
    super().__init__(bigquery_conn_id=bq_conn_id, **init_params_dict)

    self.use_legacy_sql = False
    self.dag_name = dag_name
    self.input_location = location
    self.enable_monitoring = enable_monitoring
    self.dataset_id = monitoring_dataset
    self.table_id = monitoring_table
    self.url = (f'bq://{self._get_field("project")}'
                f'.{self.dataset_id}.{self.table_id}')

    self._create_monitoring_dataset_and_table_if_not_exist()

  def get_location(self):
    """Retrieves the full url of the BigQuery data source.

    Returns:
      The full url of the data source
    """
    return self.url

  def _create_monitoring_dataset_and_table_if_not_exist(self) -> None:
    """Creates a monitoring dataset and table if doesn't exist."""
    bq_cursor = self.get_conn().cursor()
    try:
      bq_cursor.get_dataset(dataset_id=self.dataset_id,
                            project_id=bq_cursor.project_id)
    except exceptions.AirflowException:
      try:
        bq_cursor.create_empty_dataset(dataset_id=self.dataset_id,
                                       project_id=bq_cursor.project_id)
      except exceptions.AirflowException as error:
        raise errors.MonitoringDatabaseError(
            error=error, msg='Can\'t create new dataset named '
            '%s in project %s.' % (self.dataset_id, bq_cursor.project_id))

    if not self.table_exists(project_id=bq_cursor.project_id,
                             dataset_id=self.dataset_id,
                             table_id=self.table_id):
      try:
        bq_cursor.create_empty_table(
            project_id=bq_cursor.project_id, dataset_id=self.dataset_id,
            table_id=self.table_id, schema_fields=_LOG_SCHEMA_FIELDS)
      except exceptions.AirflowException as error:
        raise errors.MonitoringDatabaseError(
            error=error, msg='Can\'t create new table named '
            '%s in database %s in project %s.' % (
                self.table_id, self.dataset_id, bq_cursor.project_id))

  @retry_utils.logged_retry_on_retriable_http_airflow_exception
  def _store_monitoring_items_with_retries(
      self, rows: List[Dict[str, Any]]) -> None:
    """Stores a monitoring item in BigQuery.

    Args:
      rows: The rows to send to the monitoring DB.
    """
    if rows:
      bq_cursor = self.get_conn().cursor()
      bq_cursor.insert_all(project_id=bq_cursor.project_id,
                           dataset_id=self.dataset_id,
                           table_id=self.table_id, rows=rows)

  def _values_to_row(self, dag_name: str,
                     timestamp: str,
                     type_id: int,
                     location: str,
                     position: str,
                     info: str) -> Dict[str, Any]:
    """Prepares and formats a DB row.

    Args:
      dag_name: Airflow DAG ID that is associated with the current monitoring.
      timestamp: The log timestamp.
      type_id: The item or error number as listed in errors.MonitoringIDsMap.
      location: The specific object location of the events within the source.
      position: The events starting position within the source or any other
                additional textual information.
      info: JSON event or any other additional textual information.

    Returns:
      a JSON row of field names and their values.
    """
    values = [dag_name, timestamp, type_id, location, position, info]
    row = {}

    for value, field in zip(values, _LOG_SCHEMA_FIELDS):
      row[field['name']] = value

    row = {'json': row}

    return row

  def store_run(self, dag_name: str, location: str, timestamp: str = None,
                json_report_1: str = '', json_report_2: str = '') -> None:
    """Stores a run log-item into monitoring DB.

    Args:
      dag_name: Airflow DAG ID that is associated with the current monitoring.
      location: The run input resource location URL.
      timestamp: The log timestamp. If None, current timestamp will be used.
      json_report_1: Any run related report data in JSON format.
      json_report_2: Any run related report data in JSON format.
    """
    if timestamp is None:
      timestamp = _generate_zone_aware_timestamp()

    row = self._values_to_row(dag_name=dag_name,
                              timestamp=timestamp,
                              type_id=MonitoringEntityMap.RUN.value,
                              location=location,
                              position=json_report_1,
                              info=json_report_2)
    try:
      self._store_monitoring_items_with_retries([row])
    except exceptions.AirflowException as error:
      raise errors.MonitoringAppendLogError(error=error,
                                            msg='Failed to insert rows')

  def store_blob(self, dag_name: str, location: str,
                 position: int, num_rows: int, timestamp: str = None) -> None:
    """Stores all blobs log-item into monitoring DB.

    Args:
      dag_name: Airflow DAG ID that is associated with the current monitoring.
      location: The run input resource location URL.
      position: The events' starting position within the BigQuery table or
        Google Cloud Storage blob file.
      num_rows: Number of rows read in blob starting from start_id.
      timestamp: The log timestamp. If None, current timestamp will be used.
    """
    if timestamp is None:
      timestamp = _generate_zone_aware_timestamp()

    row = self._values_to_row(dag_name=dag_name,
                              timestamp=timestamp,
                              type_id=MonitoringEntityMap.BLOB.value,
                              location=location,
                              position=str(position),
                              info=str(num_rows))
    try:
      self._store_monitoring_items_with_retries([row])
    except exceptions.AirflowException as error:
      raise errors.MonitoringAppendLogError(error=error,
                                            msg='Failed to insert rows')

  def store_events(
      self, dag_name: str, location: str, timestamp: str = None,
      id_event_error_tuple_list: List[Tuple[int, Dict[str, Any], int]] = None
      ) -> None:
    """Stores all event log-items into monitoring DB.

    Args:
      dag_name: Airflow DAG ID that is associated with the current monitoring.
      location: The run input resource location URL.
      timestamp: The log timestamp. If None, current timestamp will be used.
      id_event_error_tuple_list: all (id, event, error_num) trupls to store.
        The tuples are a set of 3 fields:
         - id: Row IDs of events in BigQuery input table, or line numbers
           in a google cloud storage blob file.
         - event: the JSON event.
         - error: The errors.MonitoringIDsMap error ID.
    """
    if timestamp is None:
      timestamp = _generate_zone_aware_timestamp()

    rows = []
    for id_event_error_tuple in id_event_error_tuple_list:
      rows.append(self._values_to_row(
          dag_name=dag_name,
          timestamp=timestamp,
          type_id=id_event_error_tuple[2],
          location=location,
          position=str(id_event_error_tuple[0]),
          info=json.dumps(id_event_error_tuple[1])))

    try:
      self._store_monitoring_items_with_retries(rows)
    except exceptions.AirflowException as error:
      raise errors.MonitoringAppendLogError(error=error,
                                            msg='Failed to insert rows')

  def store_retry(
      self, dag_name: str, location: str, timestamp: str = None) -> None:
    """Stores a retry log-item into monitoring DB.

    Args:
      dag_name: Airflow DAG ID that is associated with the current monitoring.
      location: The run input resource location URL.
      timestamp: The log timestamp. If None, current timestamp will be used.
    """
    if timestamp is None:
      timestamp = _generate_zone_aware_timestamp()

    row = self._values_to_row(dag_name=dag_name,
                              timestamp=timestamp,
                              type_id=MonitoringEntityMap.RETRY.value,
                              location=location,
                              position='',
                              info='')
    try:
      self._store_monitoring_items_with_retries([row])
    except exceptions.AirflowException as error:
      raise errors.MonitoringAppendLogError(error=error,
                                            msg='Failed to insert retry row')

  def generate_processed_blobs_ranges(
      self) -> Generator[Tuple[Any, Any], None, None]:
    """Generates tuples of processed blobs from monitoring DB.

    Generates tuples of (position, info) for each blob with the same dag_id and
    location.

    Yields:
      Tuples of (position, info) of processed events id ranges.
    """
    sql = ('SELECT `position`, `info` '
           f'FROM `{self.dataset_id}`.`{self.table_id}` '
           'WHERE `dag_name`=%(dag_name)s '
           ' AND `location`=%(location)s '
           ' AND `type_id`=%(type_id)s '
           'ORDER BY `position`')
    bq_cursor = self.get_conn().cursor()
    bq_cursor.execute(
        sql, {
            'dag_name': self.dag_name,
            'location': self.input_location,
            'type_id': MonitoringEntityMap.BLOB.value
        })

    row = bq_cursor.fetchone()
    while row is not None:
      yield row[0], row[1]
      row = bq_cursor.fetchone()

  def events_blobs_generator(self) -> Generator[blob.Blob, None, None]:
    """Generates blobs of retriable failed events stored in monitoring.

    Retrieves all retriable failed events (of same dag_name and location) from
    the monitoring table back until the last retry or the beginning of the table
    if no previous retries found.
    A retry entity will be logged in monitoring table if is_retry is True.

    Yields:
      A blob object containing events from a page with length of
      _DEFAULT_PAGE_SIZE from the monitoring table.
    """
    sql = (
        'SELECT `info` '
        f'FROM `{self.dataset_id}`.`{self.table_id}` '
        'WHERE `dag_name`=%(dag_name)s '
        '  AND `location`=%(location)s AND `type_id`>9 '
        '  AND `type_id`<50 '
        '  AND `timestamp`>('
        '    SELECT IFNULL(`max_timestamp`, CAST("2020-1-1" AS TIMESTAMP)) '
        '    FROM (SELECT max(`timestamp`) AS `max_timestamp`'
        f'          FROM `{self.dataset_id}`.`{self.table_id}` '
        '          WHERE `type_id`=%(type_id)s '
        '                AND `dag_name`=%(dag_name)s '
        '                AND `location`=%(location)s '
        '          HAVING MAX(`timestamp`)=MAX(`timestamp`) '
        '          UNION ALL '
        '          SELECT NULL '
        '          ORDER BY `max_timestamp` DESC '
        '          LIMIT 1)'
        ')')
    bq_cursor = self.get_conn().cursor()
    bq_cursor.execute(
        sql, {
            'dag_name': self.dag_name,
            'location': self.input_location,
            'type_id': MonitoringEntityMap.BLOB.value
        })

    if self.enable_monitoring:
      self.store_retry(dag_name=self.dag_name, location=self.input_location)

    i = 0
    events = []
    row = bq_cursor.fetchone()
    while row is not None:
      events.append(json.loads(row[0]))
      i += 1

      if i == _DEFAULT_PAGE_SIZE:
        yield blob.Blob(events, self.url)
        i = 0
        events = []

      row = bq_cursor.fetchone()

    if events:
      yield blob.Blob(events, self.url)

  def cleanup_by_days_to_live(self, days_to_live: int) -> None:
    """Removes data older than days_to_live from the monitoring table.

    Args:
      days_to_live: The number of days data can live before being
      removed. Must be at least 1.
    """
    if not days_to_live:
      raise errors.MonitoringCleanupError(msg='Failed to cleanup monitoring '
                                          'table because days_to_live was not'
                                          'set.')

    if days_to_live < 1:
      raise errors.MonitoringCleanupError(msg='Failed to cleanup monitoring '
                                          'table because days_to_live was < 1'
                                          'day.')

    cutoff_timestamp = (datetime.datetime.utcnow() - datetime.timedelta(
        days=days_to_live)).isoformat() + 'Z'

    cleanup_condition = '`timestamp`<%(cutoff_timestamp)s'
    params = {'cutoff_timestamp': cutoff_timestamp}

    try:
      self._cleanup_monitoring_items_with_retries(cleanup_condition, params)
    except exceptions.AirflowException as error:
      raise errors.MonitoringCleanupError(
          error=error,
          msg='Failed to cleanup monitoring table.')

  @retry_utils.logged_retry_on_retriable_http_airflow_exception
  def _cleanup_monitoring_items_with_retries(self,
                                             cleanup_condition: str,
                                             params: Dict[str, Any]) -> None:
    """Performs the delete operation to remove data from the monitoring table.

    Args:
      cleanup_condition: The SQL clause to determine which data to remove.
      params: The params to be used in the SQL statement.
    """
    sql = (
        f'DELETE '
        f'FROM `{self.dataset_id}.{self.table_id}` '
        f'WHERE {cleanup_condition}'
    )

    bq_cursor = self.get_conn().cursor()
    bq_cursor.execute(sql, params)
