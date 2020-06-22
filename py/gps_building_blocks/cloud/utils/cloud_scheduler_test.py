# Lint as: python3
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for google3.third_party.gps_building_blocks.cloud.utils.cloud_scheduler."""

import unittest

from unittest import mock

from absl.testing import parameterized
from gps_building_blocks.cloud.utils import cloud_auth
from gps_building_blocks.cloud.utils import cloud_scheduler


class CloudSchedulerTest(parameterized.TestCase):

  def setUp(self):
    """Creates mock objects for googleapi client."""
    super(CloudSchedulerTest, self).setUp()
    self.addCleanup(mock.patch.stopall)
    self.project_id = 'project_id'

  @mock.patch.object(cloud_auth, 'build_impersonated_client', autospec=True)
  def test_client_initializes_with_impersonated_service_account(
      self, mock_impersonated_client):
    service_account_name = 'my-svc-account@project-id.iam.gserviceaccount.com'

    cloud_scheduler.CloudSchedulerUtils(
        project_id=self.project_id,
        service_account_name=service_account_name)

    mock_impersonated_client.assert_called_once_with('cloudscheduler',
                                                     service_account_name,
                                                     'v1beta1')

  def test_client_initializes_value_error(self):
    with self.assertRaises(ValueError):
      cloud_scheduler.CloudSchedulerUtils(project_id=self.project_id)

if __name__ == '__main__':
  unittest.main()
