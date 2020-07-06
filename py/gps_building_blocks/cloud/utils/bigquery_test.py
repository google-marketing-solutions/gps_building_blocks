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

"""Tests for google3.third_party.gps_building_blocks.cloud.utils.bigquery."""
import unittest
from unittest import mock

from google.auth import credentials
from google.cloud import bigquery
from absl.testing import parameterized
from gps_building_blocks.cloud.utils import bigquery as bigquery_utils
from gps_building_blocks.cloud.utils import cloud_auth


class BigQueryTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.addCleanup(mock.patch.stopall)
    # Mock for google.cloud.storage.Client object
    self.project_id = 'project-id'
    self.mock_client = mock.patch.object(
        bigquery, 'Client', autospec=True).start()
    self.mock_credentials = mock.Mock(credentials.Credentials, autospec=True)

  @mock.patch.object(cloud_auth, 'impersonate_service_account', autospec=True)
  def test_client_initializes_with_impersonated_service_account(
      self, mock_impersonated_account):
    service_account_name = 'my-svc-account@project-id.iam.gserviceaccount.com'
    mock_impersonated_account.return_value = self.mock_credentials

    bigquery_utils.BigQueryUtils(
        project_id=self.project_id,
        service_account_name=service_account_name)

    mock_impersonated_account.assert_called_once_with(service_account_name)
    self.mock_client.assert_called_with(
        project=self.project_id, credentials=self.mock_credentials)

if __name__ == '__main__':
  unittest.main()
