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

"""Tests for airflow.operators.gcs_to_ga_operator."""

from airflow.contrib.hooks import gcp_api_base_hook

from absl.testing import absltest
from absl.testing.absltest import mock
from gps_building_blocks.airflow.hooks import ga_hook
from gps_building_blocks.airflow.hooks import gcs_hook
from gps_building_blocks.airflow.operators import gcs_to_ga_operator


class GoogleCloudStorageToGoogleAnalyticsOperatorTest(absltest.TestCase):

  def test_init(self):
    with mock.patch.object(
        gcs_hook, 'GoogleCloudStorageHook',
        autospec=True), mock.patch.object(
            ga_hook, 'GoogleAnalyticsHook',
            autospec=True), mock.patch.object(
                gcp_api_base_hook.GoogleCloudBaseHook,
                '__init__', autospec=True):
      data_connector = (
          gcs_to_ga_operator.GoogleCloudStorageToGoogleAnalyticsOperator(
              task_id='task',
              gcs_bucket='bucket',
              gcs_content_type='JSON',
              gcs_prefix='prefix',
              ga_tracking_id='UA-12345-6'))

    self.assertIsInstance(data_connector.input_hook,
                          gcs_hook.GoogleCloudStorageHook)
    self.assertIsInstance(data_connector.output_hook,
                          ga_hook.GoogleAnalyticsHook)

if __name__ == '__main__':
  absltest.main()
