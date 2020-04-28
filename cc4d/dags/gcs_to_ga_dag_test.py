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

"""Tests for google3.third_party.gps_building_blocks.cc4d.dags.gcs_to_ga_dag."""

import unittest

from airflow import models
from airflow.contrib.hooks import gcp_api_base_hook
import mock

from gps_building_blocks.cc4d.dags import gcs_to_ga_dag

AIRFLOW_VARIABLES = {
    'schedule': '@once',
    'gcs_bucket_name': 'test_bucket',
    'gcs_bucket_prefix': 'test_dataset',
    'ga_tracking_id': 'UA-12345-67'
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

  def test_create_dag(self):
    """Tests that returned DAG contains correct DAG and tasks."""
    expected_task_ids = ['gcs_to_ga_task']

    with mock.patch.object(
        gcp_api_base_hook.GoogleCloudBaseHook, '__init__', autospec=True):
      dag = gcs_to_ga_dag.create_dag()

    self.assertIsInstance(dag, models.DAG)
    self.assertEqual(len(dag.tasks), len(expected_task_ids))
    for task in dag.tasks:
      self.assertIsInstance(task, models.BaseOperator)
    for idx, task_id in enumerate(expected_task_ids):
      self.assertEqual(dag.tasks[idx].task_id, task_id)


if __name__ == '__main__':
  unittest.main()
