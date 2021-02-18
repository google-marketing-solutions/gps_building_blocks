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

"""Tests for tcrm.dags.base_dag."""

import datetime
import unittest
from unittest import mock

from airflow import models
from airflow.exceptions import AirflowException

from gps_building_blocks.tcrm.dags import base_dag
from gps_building_blocks.tcrm.operators import error_report_operator


class FakeDag(base_dag.BaseDag):

  def create_task(
      self,
      main_dag: models.DAG = None,
      is_retry: bool = False) -> error_report_operator.ErrorReportOperator:
    return error_report_operator.ErrorReportOperator(
        task_id='configuration_error',
        error=Exception(),
        dag=main_dag)


class BaseDagTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.dag_name = 'test_dag'

    self.addCleanup(mock.patch.stopall)
    self.mock_variable = mock.patch.object(
        models, 'Variable', autospec=True).start()

    def mock_get(_, fallback=None):
      return fallback
    self.mock_variable.get.side_effect = mock_get
    self.dag = FakeDag(self.dag_name)

  def test_init(self):
    fake_dag = FakeDag(self.dag_name)

    dag = fake_dag.create_dag()

    self.assertEqual(dag.default_args['retries'], base_dag._DAG_RETRIES)
    self.assertEqual(
        dag.default_args['retry_delay'], datetime.timedelta(
            minutes=base_dag._DAG_RETRY_DELAY_MINUTES))
    self.assertEqual(dag.schedule_interval, base_dag._DAG_SCHEDULE)

  def test_create_dag_successfully(self):
    self.dag.create_task = mock.MagicMock()

    self.dag.create_dag()

    self.assertEqual(self.dag.create_task.call_count, 2)

  def test_create_dag_failed_due_to_config_error(self):
    self.dag.create_task = mock.MagicMock()
    self.dag.create_task.side_effect = AirflowException()

    self.dag.create_dag()

    self.assertEqual(self.dag.create_task.call_count, 1)


if __name__ == '__main__':
  unittest.main()
