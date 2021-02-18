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

"""Tests for gps_building_blocks.tcrm.dags.bq_to_ads_uac_dag."""

import unittest

from airflow import models
from airflow.contrib.hooks import bigquery_hook
import mock

from gps_building_blocks.tcrm.dags import bq_to_ads_uac_dag
from gps_building_blocks.tcrm.hooks import monitoring_hook

_DAG_NAME = bq_to_ads_uac_dag._DAG_NAME

AIRFLOW_VARIABLES = {
    'dag_name': _DAG_NAME,
    f'{_DAG_NAME}_schedule': '@once',
    f'{_DAG_NAME}_retries': 0,
    f'{_DAG_NAME}_retry_delay': 3,
    f'{_DAG_NAME}_is_retry': True,
    f'{_DAG_NAME}_is_run': True,
    f'{_DAG_NAME}_enable_run_report': False,
    f'{_DAG_NAME}_enable_monitoring': True,
    'monitoring_dataset': 'test_monitoring_dataset',
    'monitoring_table': 'test_monitoring_table',
    'monitoring_bq_conn_id': 'test_monitoring_conn',
    'bq_dataset_id': 'test_dataset',
    'bq_table_id': 'test_table',
}


class DAGTest(unittest.TestCase):

  def setUp(self):
    super(DAGTest, self).setUp()
    self.addCleanup(mock.patch.stopall)
    self.mock_variable = mock.patch.object(
        models, 'Variable', autospec=True).start()
    # `side_effect` is assigned to `lambda` to dynamically return values
    # each time when self.mock_variable is called.
    self.mock_variable.get.side_effect = (
        lambda key, value: AIRFLOW_VARIABLES[key])

    self.original_bigquery_hook_init = bigquery_hook.BigQueryHook.__init__
    bigquery_hook.BigQueryHook.__init__ = mock.MagicMock()

    self.original_monitoring_hook = monitoring_hook.MonitoringHook
    monitoring_hook.MonitoringHook = mock.MagicMock()

  def tearDown(self):
    super().tearDown()
    bigquery_hook.BigQueryHook.__init__ = self.original_bigquery_hook_init
    monitoring_hook.MonitoringHook = self.original_monitoring_hook

  def test_create_dag(self):
    """Tests that returned DAG contains correct DAG and tasks."""
    expected_task_ids = ['bq_to_ads_uac_retry_task', 'bq_to_ads_uac_task']

    dag = bq_to_ads_uac_dag.BigQueryToAdsUACDag(
        AIRFLOW_VARIABLES['dag_name']).create_dag()
    self.assertIsInstance(dag, models.DAG)
    self.assertEqual(len(dag.tasks), len(expected_task_ids))
    for task in dag.tasks:
      self.assertIsInstance(task, models.BaseOperator)
    actual_task_ids = [t.task_id for t in dag.tasks]
    self.assertListEqual(actual_task_ids, expected_task_ids)


if __name__ == '__main__':
  unittest.main()
