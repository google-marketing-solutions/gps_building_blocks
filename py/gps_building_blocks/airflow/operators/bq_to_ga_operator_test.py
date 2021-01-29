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

"""Tests for py.airflow.operators.bq_to_ga_operator."""

from airflow.contrib.hooks import bigquery_hook

from absl.testing import absltest
from absl.testing.absltest import mock
from gps_building_blocks.airflow.hooks import bq_hook
from gps_building_blocks.airflow.hooks import ga_hook
from gps_building_blocks.airflow.operators import bq_to_ga_operator


class BigQueryToGoogleAnalyticsOperatorbTest(absltest.TestCase):

  def test_init(self):
    with mock.patch.object(bigquery_hook.BigQueryHook, '__init__',
                           autospec=True):
      data_connector = (bq_to_ga_operator.
                        BigQueryToGoogleAnalyticsOperator(
                            task_id='task',
                            bq_conn_id='conn_id',
                            bq_dataset_id='dataset',
                            bq_table_id='table',
                            ga_tracking_id='UA-12345-6'))

    self.assertIsInstance(data_connector.input_hook, bq_hook.BigQueryHook)
    self.assertIsInstance(data_connector.output_hook,
                          ga_hook.GoogleAnalyticsHook)


if __name__ == '__main__':
  absltest.main()
