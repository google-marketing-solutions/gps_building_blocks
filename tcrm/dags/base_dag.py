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

"""Base Airflow DAG for TCRM workflow."""

import abc
import datetime
import logging
from typing import Any
from airflow import models
from airflow import utils
from airflow.exceptions import AirflowException

from gps_building_blocks.tcrm.operators import error_report_operator
from gps_building_blocks.tcrm.utils import errors

# Airflow DAG configurations.
_DAG_RETRIES = 0
_DAG_RETRY_DELAY_MINUTES = 3
_DAG_SCHEDULE = '@once'

# Indicates whether the tasks will return a run report or not. The report will
# be returned as the operator's output. Not all operators have reports.
_ENABLE_RETURN_REPORT = False

# Whether or not the DAG should use the monitoring storage for logging.
# Enablee monitoring to enable retry and reporting later.
_DAG_ENABLE_MONITORING = True
_DEFAULT_MONITORING_DATASET_ID = 'tcrm_monitoring_dataset'
_DEFAULT_MONITORING_TABLE_ID = 'tcrm_monitoring_table'

# BigQuery connection ID for the monitoring table. Refer to
# https://cloud.google.com/composer/docs/how-to/managing/connections
# for more details on Managing Airflow connections.
# This could be the same or different from the input BQ connection ID.
_MONITORING_BQ_CONN_ID = 'bigquery_default'

# Whether or not the DAG should include a retry task. This is an internal retry
# to send failed events from previous similar runs. It is different from the
# Airflow retry of the whole DAG.
# If True, the input resource will be the monitoring BigQuery table and dataset
# (as described in _MONITORING_DATASET and _MONITORING_TABLE). Previously failed
# events will be resent to the same output resource.
_DAG_IS_RETRY = True

# Whether or not the DAG should include a main run. This option can be used
# should the user want to skip the main run and only run the retry operation.
_DAG_IS_RUN = True


def create_error_report_task(
    dag: models.DAG,
    error: Exception) -> error_report_operator.ErrorReportOperator:
  """Creates an error task and attaches it to the DAG.

  In case there was an error during a task creation in the DAG, that task will
  be replaced with an error task. An error task will make the DAG
  fail with the appropriate error message from the initial task so the user
  can fix the issue that prevented the task creation (e.g. due to missing
  params).

  Args:
    dag: The DAG that tasks attach to.
    error: The error to display.

  Returns:
    An ErrorReportOperator instace task.
  """
  return error_report_operator.ErrorReportOperator(
      task_id='configuration_error',
      error=error,
      dag=dag)


class BaseDag(abc.ABC):
  """Base Airflow DAG.

  Attributes:
    dag_name: The name of the dag.
    dag_retries: The retry times for the dag.
    dag_retry_delay: The interval between Airflow DAG retries.
    dag_schedule: The schedule for the dag.
    dag_is_retry: Whether or not the DAG should include a retry task. This is
                  an internal retry to send faild events from previous
                  similar runs. It is different from the Airflow retry of the
                  whole DAG.
    dag_is_run: Whether or not the DAG should includ a main run.
    dag_enable_run_report: Indicates whether the tasks will return a run report
                           or not.
    dag_enable_monitoring: Whether or not the DAG should use the monitoring
                           storage for logging. Enable monitoring to enable
                           retry and reporting later.
    monitoring_dataset: Dataset id of the monitoring table.
    monitoring_table: Table name of the monitoring table.
    monitoring_bq_conn_id: BigQuery connection ID for the monitoring table.
  """

  def __init__(self, dag_name: str)  -> None:
    """Initializes the base DAG.

    Args:
      dag_name: The name of the DAG.
    """
    self.dag_name = dag_name

    self.dag_retries = int(models.Variable.get(f'{self.dag_name}_retries',
                                               _DAG_RETRIES))
    self.dag_retry_delay = int(models.Variable.get(
        f'{self.dag_name}_retry_delay', _DAG_RETRY_DELAY_MINUTES))
    self.dag_schedule = models.Variable.get(f'{self.dag_name}_schedule',
                                            _DAG_SCHEDULE)
    self.dag_is_retry = bool(int(
        models.Variable.get(f'{self.dag_name}_is_retry', _DAG_IS_RETRY)))
    self.dag_is_run = bool(int(
        models.Variable.get(f'{self.dag_name}_is_run', _DAG_IS_RUN)))

    self.dag_enable_run_report = bool(int(
        models.Variable.get(f'{self.dag_name}_enable_run_report',
                            _ENABLE_RETURN_REPORT)))

    self.dag_enable_monitoring = bool(int(
        models.Variable.get(f'{self.dag_name}_enable_monitoring',
                            _DAG_ENABLE_MONITORING)))
    self.monitoring_dataset = models.Variable.get(
        'monitoring_dataset', _DEFAULT_MONITORING_DATASET_ID)
    self.monitoring_table = models.Variable.get(
        'monitoring_table', _DEFAULT_MONITORING_TABLE_ID)
    self.monitoring_bq_conn_id = models.Variable.get(
        'monitoring_bq_conn_id', _MONITORING_BQ_CONN_ID)

  def _initialize_dag(self) -> models.DAG:
    """Initializes an Airflow DAG with appropriate default args.

    Returns:
      models.DAG: Instance models.DAG.
    """
    logging.info('Running pipeline at schedule %s.', self.dag_schedule)

    default_args = {
        'retries': self.dag_retries,
        'retry_delay': datetime.timedelta(minutes=self.dag_retry_delay),
        'start_date': utils.dates.days_ago(1)
    }

    return models.DAG(
        dag_id=self.dag_name,
        schedule_interval=self.dag_schedule,
        default_args=default_args)

  @abc.abstractmethod
  def create_task(
      self,
      main_dag: models.DAG = None,
      is_retry: bool = False) -> models.BaseOperator:
    """Creates a task and attaches it to the DAG.

    Args:
      main_dag: The DAG that tasks attach to.
      is_retry: Whether or not the operator should include a retry task.

    Returns:
      An instance of models.BaseOperator.
    """
    pass

  def _try_create_task(self, main_dag: models.DAG, is_retry: bool) -> Any:
    """Tries to create an Airflow task.

    Args:
      main_dag: The DAG that tasks attach to.
      is_retry: Whether or not the operator should include a retry task.

    Raises:
      DAGError raised when task is failed to create.

    Returns:
      Airflow task instance.
    """
    try:
      return self.create_task(main_dag=main_dag, is_retry=is_retry)
    except (errors.DataOutConnectorValueError,
            errors.DataInConnectorValueError,
            AirflowException,
            ValueError) as error:
      raise errors.DAGError(error=error, msg='Couldn\'t create task.')

  def create_dag(self) -> models.DAG:
    """Creates the DAG.

    Returns:
      Airflow DAG instance.
    """

    dag = self._initialize_dag()

    try:
      if self.dag_is_retry:
        retry_task = self._try_create_task(main_dag=dag, is_retry=True)
      if self.dag_is_run:
        run_task = self._try_create_task(main_dag=dag, is_retry=False)
        if self.dag_is_retry:
          run_task.set_upstream(retry_task)
    except errors.DAGError as error:
      dag = self._initialize_dag()
      create_error_report_task(dag=dag, error=error)

    return dag

  def get_task_id(self, task_name: str, is_retry: bool) -> str:
    """Gets task_id by task type.

    Args:
      task_name: The name of the task.
      is_retry: Whether or not the operator should include a retry task.

    Returns:
      Task id.
    """
    if is_retry:
      return task_name + '_retry_task'
    else:
      return task_name + '_task'
