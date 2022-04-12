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

"""Tests for gps_building_blocks.cloud.utils.cloud_api."""

from google.auth import credentials
import requests

from absl.testing import absltest
from absl.testing import parameterized
from absl.testing.absltest import mock
from gps_building_blocks.cloud.utils import cloud_api
from gps_building_blocks.cloud.utils import cloud_auth
from gps_building_blocks.cloud.utils import utils


class CloudApiTest(parameterized.TestCase):

  def setUp(self):
    """Setup for CloudApiTest."""
    super(CloudApiTest, self).setUp()
    self.addCleanup(mock.patch.stopall)
    self.project_id = 'project_id'
    self.service_account_name = 'service_account_name'
    self.api_name = 'storage'
    self.session = mock.Mock()
    self.mock_client = mock.Mock()
    self.parent = f'projects/{self.project_id}'
    self.service_account_key_file = '/path/to/service/account/key'
    self.mock_credentials = mock.Mock(credentials.Credentials, autospec=True)
    # Mocks for building client.
    mock_get_credentials = mock.patch.object(
        cloud_auth, 'get_credentials', autospec=True).start()
    mock_build_service_client = mock.patch.object(
        cloud_auth, 'build_service_client', autospec=True).start()
    mock_get_credentials.return_value = self.mock_credentials
    mock_build_service_client.return_value = self.mock_client
    # Mocks for operations.
    self.operation_client = mock.Mock()
    self.mock_wait_for_operation = mock.patch.object(
        utils, 'wait_for_operation', autospec=True).start()
    self.mock_execute_request = mock.patch.object(
        utils, 'execute_request', autospec=True).start()
    self.operation = {}
    self.mock_execute_request.return_value = self.operation
    self.mock_client.operations.return_value = self.operation_client
    self.cloud_api_utils = cloud_api.CloudApiUtils(
        self.project_id, service_account_key_file=self.service_account_key_file)

  @mock.patch.object(cloud_auth, 'build_impersonated_client', autospec=True)
  def test_client_initializes_with_impersonated_service_account(
      self, mock_impersonated_client):
    service_account_name = 'my-svc-account@project-id.iam.gserviceaccount.com'
    version = 'v1beta1'

    cloud_api.CloudApiUtils(
        self.project_id, service_account_name=service_account_name)

    mock_impersonated_client.assert_called_once_with('serviceusage',
                                                     service_account_name,
                                                     version)

  def test_enable_cloud_apis(self):
    apis = [self.api_name]
    body = {'serviceIds': apis}
    mock_batch_enable = self.mock_client.services.return_value.batchEnable

    self.cloud_api_utils.enable_apis(apis)

    mock_batch_enable.assert_called_once_with(parent=self.parent, body=body)
    self.mock_wait_for_operation.assert_called_once_with(
        self.operation_client, self.operation)

  def test_disable_cloud_api(self):
    cloud_api.disable_api(self.session, self.project_id, self.api_name)

    disable_url = '{}/{}/services/{}:disable'.format(cloud_api._SERVICE_URL,
                                                     self.project_id,
                                                     self.api_name)
    self.session.post.assert_called_once_with(
        disable_url, {'disableDependentServices': True})

  @parameterized.named_parameters(
      ('true', 'ENABLED', True),
      ('false', 'DISABLED', False),
  )
  def test_if_cloud_api_is_api_enabled(self, status, expected):
    response = mock.Mock()
    response.content = '{{"state": "{}"}}'.format(status)
    self.session.get.return_value = response

    is_api_enabled = cloud_api.is_api_enabled(self.session, self.project_id,
                                              self.api_name)

    self.assertEqual(is_api_enabled, expected)
    self.session.get.assert_called_once_with('{}/{}/services/{}'.format(
        cloud_api._SERVICE_URL, self.project_id, self.api_name))

  def test_post_request_throws_error(self):
    response = requests.Response()
    response.status_code = 400
    self.session.post.return_value = response
    url = 'http://google.com'
    data = {}

    with self.assertRaises(cloud_api.Error):
      cloud_api.post_request(self.session, url, data)


if __name__ == '__main__':
  absltest.main()
