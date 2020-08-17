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

"""Custom BigQuery hook to generate BigQuery table pages as blobs."""

from typing import Any, Dict, Generator, List, Optional, Text

from airflow.contrib.hooks import bigquery_hook
from googleapiclient import errors as googleapiclient_errors

from gps_building_blocks.airflow.hooks import input_hook_interface
from gps_building_blocks.airflow.utils import blob
from gps_building_blocks.airflow.utils import errors
from gps_building_blocks.airflow.utils import retry_utils

_DEFAULT_PAGE_SIZE = 1000
_PLATFORM = 'BigQuery'


class BigQueryHook(
    bigquery_hook.BigQueryHook, input_hook_interface.InputHookInterface):
  """Custom BigQuery hook to generate table pages as blobs.

  Attributes:
    dataset_id: Unique name of the dataset.
    table_id: Unique location within the dataset.
    selected_fields: Subset of fields to return.
    url: URL of data, formatted as 'bq://{project_id}.{dataset_id}.{table.id}'.
  """

  def __init__(self,
               conn_id: Text,
               dataset_id: Text,
               table_id: Text,
               selected_fields: Optional[Text] = None,
               **kwargs) -> None:
    """Initializes the generator of a specified BigQuery table.

    Args:
      conn_id: Connection id passed to airflow's BigQueryHook.
      dataset_id: Dataset id of the target table.
      table_id: Table name of the target table.
      selected_fields: Subset of fields to return. Example: 'field_1,field_2'.
      **kwargs: Other arguments to pass through to Airflow's BigQueryHook.
    """
    super(BigQueryHook, self).__init__(bigquery_conn_id=conn_id, **kwargs)
    self.dataset_id = dataset_id
    self.table_id = table_id
    self.selected_fields = selected_fields
    self.url = 'bq://{}.{}.{}'.format(
        self._get_field('project'), self.dataset_id, self.table_id)

  def _str_to_bq_type(self, bq_str: Text, bq_type: Text) -> Any:
    """Casts BigQuery string row data to the appropriate BigQuery data type.

    Args:
      bq_str: String data to be cast to target type.
      bq_type: Target data type, e.g. BOOLEAN, INTEGER, FLOAT, TIMESTAMP.

    Returns:
      Typed data cast from string. None when input data is None.
    """
    if bq_str is None:
      return None
    elif bq_type == 'BOOLEAN':
      if bq_str.lower() not in ['true', 'false']:
        raise ValueError("{} must have value 'true' or 'false'".format(
            bq_str))
      return bq_str.lower() == 'true'
    elif bq_type == 'INTEGER':
      return int(bq_str)
    elif bq_type == 'FLOAT' or bq_type == 'TIMESTAMP':
      return float(bq_str)
    else:
      return bq_str

  def _query_results_to_blob(
      self, query_results: Dict[Text, Any], start_index: int) -> blob.Blob:
    """Converts query results of BigQuery to event blob.

    Args:
      query_results: Raw query results.
      start_index: Start index of BigQuery table rows.

    Returns:
      blob: Event blob containing event list and status.
    """
    blob_unique_id = '{}/{}'.format(self.url, start_index)
    if query_results is None:
      return blob.Blob(events=[], blob_id=blob_unique_id,
                       platform=_PLATFORM, source=self.dataset_id,
                       location=self.table_id, position=start_index,
                       status=blob.BlobStatus.ERROR,
                       status_desc='Unable to get the blob at {} in {}.'.format(
                           start_index, self.url))

    events = self._query_results_to_maps_list(query_results)
    return blob.Blob(events=events, blob_id=blob_unique_id,
                     platform=_PLATFORM, source=self.dataset_id,
                     location=self.table_id, position=start_index)

  def _query_results_to_maps_list(
      self, query_results: Dict[Text, Any]) -> List[Dict[Text, Any]]:
    """Converts table rows query results of BigQuery to list of maps.

    Args:
      query_results: Raw query result.

    Returns:
      data: Table rows in the format of list of maps.
    """
    fields = [field['name'] for field in query_results['schema']['fields']]
    col_types = [field['type'] for field in query_results['schema']['fields']]
    rows = query_results.get('rows', [])
    batch_data = []
    for row in rows:
      values = [cell['v'] for cell in row['f']]
      typed_values = [self._str_to_bq_type(value, type)
                      for value, type in zip(values, col_types)]
      data = dict(zip(fields, typed_values))
      batch_data.append(data)
    return batch_data

  @retry_utils.logged_retry_on_retriable_http_error
  def _get_tabledata_with_retries(self, bq_cursor: bigquery_hook.BigQueryCursor,
                                  start_index: int) -> Dict[Text, Any]:
    """Attempt to get BigQuery table data with retries.

    Args:
      bq_cursor: BigQuery Cursor instance.
      start_index: Zero based index of the starting row to read.

    Returns:
      query_results: Map containing the requested rows.
    """
    query_results = bq_cursor.get_tabledata(
        dataset_id=self.dataset_id,
        table_id=self.table_id,
        max_results=_DEFAULT_PAGE_SIZE,
        start_index=start_index,
        selected_fields=self.selected_fields)
    if query_results and not query_results.get('schema'):
      query_results['schema'] = bq_cursor.get_schema(self.dataset_id,
                                                     self.table_id)
    return query_results

  def list_tables(self,
                  dataset_id: Optional[Text] = None,
                  prefix: Text = '') -> List[Text]:
    """Lists table ids in specified dataset filtered by specified prefix.

    Args:
      dataset_id: Dataset id of which the tables to list.
      prefix: Prefix of the table id to list.

    Returns:
      table_ids: List of table ids.
    """
    if not dataset_id:
      dataset_id = self.dataset_id
    bq_cursor = self.get_conn().cursor()
    tables_list_resp = bq_cursor.service.tables().list(
        projectId=bq_cursor.project_id,
        datasetId=dataset_id,
        maxResults=_DEFAULT_PAGE_SIZE).execute()
    result = []
    while True:
      for table in tables_list_resp.get('tables', []):
        if table['tableReference']['tableId'].startswith(prefix):
          result.append(table['tableReference']['tableId'])
      if tables_list_resp.get('nextPageToken'):
        tables_list_resp = bq_cursor.service.tables().list(
            projectId=bq_cursor.project_id,
            datasetId=dataset_id,
            maxResults=_DEFAULT_PAGE_SIZE,
            pageToken=tables_list_resp['nextPageToken']).execute()
      else:
        break
    return result

  def events_blobs_generator(self) -> Generator[blob.Blob, None, None]:
    """Generates pages of specified BigQuery table as blobs.

    Yields:
      blob: A blob object containing events from a page with length of
      _DEFAULT_PAGE_SIZE from the specified BigQuery table.

    Raises:
      DataInConnectorError: Raised when BigQuery table data cannot be accessed.
    """
    start_index = 0
    total_rows = -1
    bq_cursor = self.get_conn().cursor()

    # Get the first page to ensure the accessibility.
    try:
      query_results = self._get_tabledata_with_retries(bq_cursor=bq_cursor,
                                                       start_index=start_index)
    except googleapiclient_errors.HttpError as error:
      raise errors.DataInConnectorError(error=error, msg=str(error))
    else:
      if query_results is None:
        raise errors.DataInConnectorError(
            msg='Unable to get any blobs in {}.'.format(self.url))
      try:
        total_rows = int(query_results.get('totalRows'))
      except (AttributeError, TypeError, ValueError):
        raise errors.DataInConnectorError(
            msg='Unable to get total rows in {}.'.format(self.url))
      else:
        yield self._query_results_to_blob(query_results, start_index)
        start_index = start_index + _DEFAULT_PAGE_SIZE

    # Get the remaining pages of the requested table.
    while start_index < total_rows:
      try:
        query_results = self._get_tabledata_with_retries(
            bq_cursor=bq_cursor, start_index=start_index)
      except googleapiclient_errors.HttpError as error:
        # Generate a blob with error status.
        blob_unique_id = '{}/{}'.format(self.url, start_index)
        yield blob.Blob(events=[], blob_id=blob_unique_id,
                        platform=_PLATFORM, source=self.dataset_id,
                        location=self.table_id, position=start_index,
                        status=blob.BlobStatus.ERROR, status_desc=str(error))
      else:
        yield self._query_results_to_blob(query_results, start_index)
      finally:
        start_index = start_index + _DEFAULT_PAGE_SIZE
