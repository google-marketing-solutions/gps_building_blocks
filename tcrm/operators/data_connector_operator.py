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

"""Data Connector Operator to send data from input source to output source."""

from typing import Any, List, Dict, Optional

from airflow import models

from gps_building_blocks.tcrm.hooks import monitoring_hook as monitoring
from gps_building_blocks.tcrm.utils import errors
from gps_building_blocks.tcrm.utils import hook_factory


class DataConnectorOperator(models.BaseOperator):
  """Custom Operator to send data from an input hook to an output hook."""

  def __init__(self, *args,
               input_hook: hook_factory.InputHookType,
               output_hook: hook_factory.OutputHookType,
               dag_name: str,
               monitoring_dataset: str = '',
               monitoring_table: str = '',
               monitoring_bq_conn_id: str = '',
               return_report: bool = False,
               enable_monitoring: bool = True,
               is_retry: bool = False,
               **kwargs) -> None:
    """Initiates the DataConnectorOperator.

    Args:
      *args: arguments for the operator.
      input_hook: The type of the input hook.
      output_hook: The type of the output hook.
      dag_name: The ID of the current running dag.
      monitoring_dataset: Dataset id of the monitoring table.
      monitoring_table: Table name of the monitoring table.
      monitoring_bq_conn_id: BigQuery connection ID for the monitoring table.
      return_report: Indicates whether to return a run report or not.
      enable_monitoring: If enabled, data transfer monitoring log will be
          stored in Storage to allow for retry of failed events.
      is_retry: If true, the operator will draw failed events from monitoring
          log and will send them to the output hook.
      **kwargs: Other arguments to pass through to the operator or hooks.
    """
    super().__init__(*args, **kwargs)

    self.dag_name = dag_name
    self.input_hook = hook_factory.get_input_hook(input_hook, **kwargs)
    self.output_hook = hook_factory.get_output_hook(output_hook, **kwargs)
    self.return_report = return_report
    self.enable_monitoring = enable_monitoring
    self.is_retry = is_retry

    if enable_monitoring and not all([monitoring_dataset,
                                      monitoring_table,
                                      monitoring_bq_conn_id]):
      raise errors.MonitoringValueError(
          msg=('Missing or empty monitoring parameters although monitoring is '
               'enabled.'),
          error_num=errors.ErrorNameIDMap.MONITORING_HOOK_INVALID_VARIABLES)

    self.monitor = monitoring.MonitoringHook(
        bq_conn_id=monitoring_bq_conn_id,
        enable_monitoring=enable_monitoring, dag_name=dag_name,
        monitoring_dataset=monitoring_dataset,
        monitoring_table=monitoring_table,
        location=self.input_hook.get_location())

  def execute(self, context: Dict[str, Any]) -> Optional[List[Any]]:
    """Executes this Operator.

    Retrieves all blobs with from input_hook and sends them to output_hook.
    Updates Storage with each blob's status upon success or failure.

    Args:
      context: Unused.

    Returns:
      A list of tuples of any data returned from output_hook if return_report
      flag is set to True.
    """
    if self.is_retry:
      blob_generator = self.monitor.events_blobs_generator()
    else:
      processed_blobs_generator = self.monitor.generate_processed_blobs_ranges()
      blob_generator = self.input_hook.events_blobs_generator(
          processed_blobs_generator=processed_blobs_generator)

    reports = []
    for blb in blob_generator:
      if blb:
        blb = self.output_hook.send_events(blb)
        reports.append(blb.reports)

        if self.enable_monitoring:
          self.monitor.store_blob(dag_name=self.dag_name,
                                  location=blb.location,
                                  position=blb.position,
                                  num_rows=blb.num_rows)
          self.monitor.store_events(dag_name=self.dag_name,
                                    location=blb.location,
                                    id_event_error_tuple_list=blb.failed_events)

    if self.return_report:
      return reports
