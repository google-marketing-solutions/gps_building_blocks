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

"""Airflow DAG for TCRM workflow.

This DAG will cleanup data in the tcrm_monitoring_dataset.tcrm_monitoring_table
BigQuery table.

This DAG relies on the following Airflow variables:

* `monitoring_data_days_to_live`: The number of days data can live before being
     removed. Default is 50 days.

Refer to https://airflow.apache.org/concepts.html#variables for more on Airflow
Variables.
"""

import os
from airflow import models
from airflow.exceptions import AirflowException

from gps_building_blocks.tcrm.dags import base_dag
from gps_building_blocks.tcrm.operators import monitoring_cleanup_operator


# Airflow configuration variables.
_AIRFLOW_ENV = 'AIRFLOW_HOME'

# Airflow DAG configurations.
_DAG_NAME = 'tcrm_monitoring_cleanup'
_DAG_SCHEDULE = '@once'

# Number of days data can live in the monitoring table before being removed.
_DEFAULT_MONITORING_DATA_DAYS_TO_LIVE = 50


class MonitoringCleanupDag(base_dag.BaseDag):
  """DAG to run monitoring table cleanup."""

  def __init__(self, dag_name: str):
    """Initializes the base DAG.

    Args:
      dag_name: The name of the DAG.
    """
    super().__init__(dag_name)
    self.days_to_live = int(
        models.Variable.get('monitoring_data_days_to_live',
                            _DEFAULT_MONITORING_DATA_DAYS_TO_LIVE))

  def create_dag(self) -> models.DAG:
    """Creates the monitoring cleanup DAG and attaches the cleanup task.

    Returns:
      An Airflow DAG with an attached monitoring cleanup task.
    """
    main_dag = self._initialize_dag()

    try:
      self.create_task(main_dag=main_dag)
    except (AirflowException, ValueError) as error:
      # Reinitializes DAG so it won't contain two tasks
      main_dag = self._initialize_dag()
      base_dag.create_error_report_task(main_dag, error)

    return main_dag

  def create_task(self, main_dag: models.DAG = None, is_retry: bool = False
                 ) -> monitoring_cleanup_operator.MonitoringCleanupOperator:
    """Creates and initializes the cleanup task.

    Args:
      main_dag: The dag that the task attaches to.
      is_retry: Whether or not the operator should includ a retry task.

    Returns:
      MonitoringCleanupOperator.
    """
    return monitoring_cleanup_operator.MonitoringCleanupOperator(
        task_id='monitoring_cleanup_task',
        monitoring_bq_conn_id=self.monitoring_bq_conn_id,
        monitoring_dataset=self.monitoring_dataset,
        monitoring_table=self.monitoring_table,
        days_to_live=self.days_to_live,
        dag_name=_DAG_NAME,
        dag=main_dag)


if os.getenv(_AIRFLOW_ENV):
  dag = MonitoringCleanupDag(_DAG_NAME).create_dag()
