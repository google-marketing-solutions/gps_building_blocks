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

"""Tests for gps_building_blocks.tcrm.operators.monitoring_cleanup_operator."""

import unittest
from unittest import mock

from gps_building_blocks.tcrm.operators import monitoring_cleanup_operator as monitoring_cleanup_operator_lib


class MonitoringCleanupOperatorTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.test_operator_kwargs = {'task_id': 'test_task_id',
                                 'tcrm_gcs_to_ga_schedule': '@once',
                                 'ga_tracking_id': 'UA-12345-67',
                                 'ga_base_params': {'v': '1'},
                                 'gcs_bucket': 'test_bucket',
                                 'gcs_prefix': 'test_dataset',
                                 'gcs_content_type': 'JSON'}
    self.monitoring_hook_path = (
        'google3.third_party.gps_building_blocks.tcrm.hooks'
        '.monitoring_hook.MonitoringHook')

  def test_execute_calls_cleanup_with_time_to_live(self):
    days_to_live = 1
    with mock.patch(
        self.monitoring_hook_path, autospec=True) as mock_monitoring_hook:
      monitoring_cleanup_operator = \
          monitoring_cleanup_operator_lib.MonitoringCleanupOperator(
              monitoring_bq_conn_id='dummy-connection',
              days_to_live=1,
              monitoring_dataset='dummy-monitoring-dataset',
              monitoring_table='dummy-monitoring-table',
              **self.test_operator_kwargs)

      monitoring_cleanup_operator.execute(None)

      mock_instance = mock_monitoring_hook.return_value
      mock_instance.cleanup_by_days_to_live.assert_called_once_with(
          days_to_live)


if __name__ == '__main__':
  unittest.main()
