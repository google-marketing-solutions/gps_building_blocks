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

"""System test for dags.bq_to_ga_dag."""

import datetime
import json
import os

from google.cloud import datastore
import pytest

from gps_building_blocks.tcrm.utils import system_testing_utils

_AIRFLOW_HOME = 'AIRFLOW_HOME'
_TEST_DAG_NAME = 'tcrm_bq_to_ga'
_TEST_TASK_NAME = 'bq_to_ga_retry_task'
_BQ_PROJECT_ID = 'bq_project_id'
_BQ_DATASET_ID = 'bq_dataset_id'
_BQ_TABLE_ID = 'bq_table_id'
_GA_TRACKING_ID = 'ga_tracking_id'
_BQ_CONN_DEFAULT = 'bigquery_default'
_GCD_CONN_DEFAULT = 'google_cloud_datastore_default'
_IS_RETRY = _TEST_DAG_NAME + '_is_retry'
_IS_RUN = _TEST_DAG_NAME + '_is_run'
_MONITORING_DATASET_KEY = 'monitoring_dataset'
_MONITORING_DATATABLE_KEY = 'monitoring_table'
_MONITORING_DATASET_VALUE = 'system_test'
_MONITORING_DATATABLE_VALUE = 'tcrm_monitoring_table'


@pytest.fixture(name='configuration')
def fixture_configuration():
  client = datastore.Client()
  kind = 'configuration'
  name = 'bq_to_ga'
  namespace = 'tcrm-system-test'
  entity_key = client.key(kind, name, namespace=namespace)
  entity = client.get(entity_key)
  return entity


@pytest.mark.systemtest
@pytest.mark.skipif(os.getenv('TEST_TYPE') != 'SYSTEM',
                    reason='Test isn''t running in system test environment.')
def test_for_retry_system(configuration):
  system_testing_utils.create_or_update_airflow_variable(
      _BQ_DATASET_ID, configuration[_BQ_DATASET_ID])
  system_testing_utils.create_or_update_airflow_variable(
      _BQ_TABLE_ID, configuration[_BQ_TABLE_ID])
  system_testing_utils.create_or_update_airflow_variable(
      _GA_TRACKING_ID, configuration[_GA_TRACKING_ID])
  system_testing_utils.create_or_update_airflow_variable(
      _IS_RETRY, 1)
  system_testing_utils.create_or_update_airflow_variable(
      _IS_RUN, 0)
  system_testing_utils.create_or_update_airflow_variable(
      _MONITORING_DATASET_KEY, _MONITORING_DATASET_VALUE)
  system_testing_utils.create_or_update_airflow_variable(
      _MONITORING_DATATABLE_KEY, _MONITORING_DATATABLE_VALUE)

  project_id = configuration[_BQ_PROJECT_ID]
  key_path = os.path.join(
      system_testing_utils.get_airflow_home(), 'key.json')

  system_testing_utils.create_or_update_airflow_gcp_connection(
      _BQ_CONN_DEFAULT, project_id, key_path)
  system_testing_utils.create_or_update_airflow_gcp_connection(
      _GCD_CONN_DEFAULT, project_id, key_path)

  source_table_id = (project_id + '.' + configuration[_BQ_DATASET_ID] + '.' +
                     configuration[_BQ_TABLE_ID])

  input_location = 'bq://' + source_table_id
  error_log_rows = []
  error_timestamp = str(datetime.datetime.utcnow()
                        - datetime.timedelta(hours=1))
  for i in range(0, 10):
    info = {
        'cid': str(10000 + i),
        't': 'event',
        'ec': 'video',
        'ea': 'play',
        'el': 'holiday',
        'ev': str(480 + i)
    }

    error_log = {
        'dag_name': _TEST_DAG_NAME,
        'timestamp': error_timestamp,
        'type_id': 10,
        'location': input_location,
        'position': i,
        'info': json.dumps(info)
    }

    error_log_rows.append(error_log)

  monitoring_table_id = (project_id + '.' + _MONITORING_DATASET_VALUE + '.' +
                         _MONITORING_DATATABLE_VALUE)
  system_testing_utils.insert_rows_to_table(
      error_log_rows, monitoring_table_id)

  execution_date = system_testing_utils.get_iso8601_date_str(
      datetime.datetime.now() - datetime.timedelta(days=2))

  system_testing_utils.run_airflow_task(
      _TEST_DAG_NAME,
      _TEST_TASK_NAME,
      execution_date)

  log_content = system_testing_utils.get_latest_task_log(
      _TEST_DAG_NAME,
      _TEST_TASK_NAME,
      execution_date)

  print(log_content)

  assert 'Task exited with return code 0' in log_content
