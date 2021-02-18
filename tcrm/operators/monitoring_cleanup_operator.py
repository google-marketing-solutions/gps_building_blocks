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

"""Operator to clean up monitoring table data."""
from typing import Any, Dict

from airflow import models

from gps_building_blocks.tcrm.hooks import monitoring_hook as monitoring_hook_lib


class MonitoringCleanupOperator(models.BaseOperator):
  """Operator to clean up monitoring table data."""

  def __init__(self, *args,
               monitoring_bq_conn_id: str,
               days_to_live: int,
               monitoring_dataset: str,
               monitoring_table: str,
               **kwargs) -> None:
    """Initializes the MonitoringCleanupOperator.

    Args:
      *args: arguments for the operator.
      monitoring_bq_conn_id: Optional;  BigQuery connection ID for the
        monitoring table. Default is 'bigquery_default'
      days_to_live: Optional; The number of days data can live before being
        removed. Default is 50 days.
      monitoring_dataset: Dataset id of the monitoring table.
      monitoring_table: Table name of the monitoring table.
      **kwargs: Other arguments to pass through to the operator or hooks.
    """
    super().__init__(*args, **kwargs)
    self.days_to_live = days_to_live
    self.monitoring_hook = monitoring_hook_lib.MonitoringHook(
        bq_conn_id=monitoring_bq_conn_id,
        monitoring_dataset=monitoring_dataset,
        monitoring_table=monitoring_table)

  def execute(self, context: Dict[str, Any]) -> None:
    """Calls the monitor cleanup methods to delete monitoring table data.

    Args:
      context: Unused.
    """
    self.monitoring_hook.cleanup_by_days_to_live(self.days_to_live)
