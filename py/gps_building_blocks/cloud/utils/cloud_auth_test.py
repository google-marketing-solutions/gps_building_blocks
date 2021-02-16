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

"""Tests for auth.py."""

import base64
import io
import os

from google import auth
from google.auth import impersonated_credentials
from google.oauth2 import service_account
from googleapiclient import discovery
from googleapiclient import errors

from absl.testing import absltest
from absl.testing.absltest import mock
from gps_building_blocks.cloud.utils import cloud_auth


class AuthTest(absltest.TestCase):

  def setUp(self):
    """Creates mock objects for googleapi client."""
    super(AuthTest, self).setUp()
    self.addCleanup(mock.patch.stopall)
    self.project_id = 'project_id'
    self.service_account_name = 'service_account_name'
    self.file_name = 'file_name'
    self.role_name = 'editor'
    self.mock_auth_default = mock.patch.object(
        auth, 'default', autospec=True).start()
    self.mock_client = mock.patch.object(
        discovery, 'build', autospec=True).start()
    self.mock_service_client = (
        self.mock_client.return_value.projects.return_value.serviceAccounts)
    self.mock_credentials = mock.Mock(spec=service_account.Credentials)
    self.mock_auth_default.return_value = (self.mock_credentials,
                                           self.project_id)

  def test_build_service_client(self):
    cloud_auth.build_service_client('service_name', self.mock_credentials)

    self.mock_client.assert_called_once_with(
        'service_name',
        'v1',
        credentials=self.mock_credentials,
        cache_discovery=False)

  @mock.patch.object(os.path, 'isfile', autospec=True)
  @mock.patch.object(service_account.Credentials, 'from_service_account_file')
  def test_get_credentials_for_service_account(self,
                                               mock_from_service_account_file,
                                               mock_is_file):
    mock_is_file.return_value = True
    mock_from_service_account_file.return_value = self.mock_credentials
    credentials = cloud_auth.get_credentials('/tmp/key.json')

    self.mock_auth_default.assert_not_called()
    self.assertEqual(self.mock_credentials, credentials)

  @mock.patch.object(os.path, 'isfile', autospec=True)
  def test_exception_is_raised_when_service_account_key_file_is_not_found(
      self, mock_is_file):
    mock_is_file.return_value = False

    with self.assertRaises(FileNotFoundError):
      cloud_auth.get_credentials('/tmp/invalid_file')

  @mock.patch.object(service_account.Credentials, 'from_service_account_info')
  def test_get_credentials_from_service_account_info(
      self, mock_from_service_account_info):
    mock_from_service_account_info.return_value = self.mock_credentials
    credentials = cloud_auth.get_credentials_from_info({
        'type': 'service_account',
        'project_id': 'google.com:fake_project',
        'private_key_id': '',
        'private_key': '',
        'client_email': '',
        'client_id': '',
        'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
        'token_uri': 'https://accounts.google.com/o/oauth2/token',
        'auth_provider_x509_cert_url':
            'https://www.googleapis.com/oauth2/v1/certs',
        'client_x509_cert_url': ''
    })

    self.mock_auth_default.assert_not_called()
    self.assertEqual(self.mock_credentials, credentials)

  @mock.patch.object(service_account.Credentials, 'from_service_account_info')
  def test_exception_is_raised_when_service_account_info_in_wrong_format(
      self, mock_from_service_account_info):
    mock_from_service_account_info.side_effect = ValueError()
    with self.assertRaises(ValueError):
      cloud_auth.get_credentials_from_info({})

  @mock.patch.object(cloud_auth, 'build_service_client', autospec=True)
  def test_iam_client(self, mock_build_service_client):
    cloud_auth._get_iam_client()

    mock_build_service_client.assert_called_once_with(
        'iam', service_account_credentials=self.mock_credentials)

  @mock.patch.object(cloud_auth, 'build_service_client', autospec=True)
  def test_resource_manager_client(self, mock_build_service_client):
    cloud_auth._get_resource_manager_client()

    mock_build_service_client.assert_called_once_with(
        'cloudresourcemanager',
        service_account_credentials=self.mock_credentials)

  @mock.patch.object(cloud_auth, 'set_service_account_role', autospec=True)
  @mock.patch.object(cloud_auth, 'create_service_account_key', autospec=True)
  def test_service_account_creation(self, mock_create_sa_key, mock_set_sa_role):
    """Test that service account is created for valid input."""
    expected_account = object()
    (self.mock_service_client.return_value.get.return_value.execute.return_value
    ) = None
    mock_create = self.mock_service_client.return_value.create
    mock_create.return_value.execute.return_value = expected_account

    actual_account = cloud_auth.create_service_account(
        self.project_id, self.service_account_name, self.role_name,
        self.file_name)

    self.assertEqual(expected_account, actual_account)
    mock_create.assert_called_once()
    mock_create_sa_key.assert_called_once_with(self.project_id,
                                               self.service_account_name,
                                               self.file_name)
    mock_set_sa_role.assert_called_once_with(self.project_id,
                                             self.service_account_name,
                                             self.role_name)

  def test_service_account_is_not_created_if_already_exists(self):
    """Test service account is not created if the account already exists."""
    expected_account = object()
    (self.mock_service_client.return_value.get.return_value.execute.return_value
    ) = expected_account
    mock_create = self.mock_service_client.return_value.create

    actual_account = cloud_auth.create_service_account(
        self.project_id, self.service_account_name, self.role_name,
        self.file_name)

    self.assertEqual(expected_account, actual_account)
    mock_create.assert_not_called()

  def test_empty_service_account_name_error(self):
    """Test that error is raised for invalid servic account name."""
    with self.assertRaises(ValueError):
      cloud_auth.create_service_account(self.project_id, '', self.role_name,
                                        self.file_name)

  def test_empty_service_account_key_file_name_name_error(self):
    """Test that error is raised for invalid servic account name."""
    with self.assertRaises(ValueError):
      cloud_auth.create_service_account(self.project_id, self.project_id,
                                        self.role_name, '')

  def test_get_service_account(self):
    """Test that service account is returned for valid account name."""
    expected_account = object()
    mock_get = self.mock_service_client.return_value.get
    mock_get.return_value.execute.return_value = expected_account

    actual_account = cloud_auth.get_service_account(self.project_id,
                                                    self.service_account_name)

    self.assertEqual(expected_account, actual_account)

  def test_get_service_account_returns_none_when_no_account_found(self):
    """Test that returned service account is none when account is not found."""

    mock_get = self.mock_service_client.return_value.get
    response = mock.Mock()
    response.status = 404
    mock_get.return_value.execute.side_effect = errors.HttpError(response, b'')

    account = cloud_auth.get_service_account(self.project_id,
                                             self.service_account_name)

    self.assertIsNone(account)

  def test_get_service_account_throws_error(self):
    """Test that error is rethrown for all unexpected http errors."""

    mock_get = self.mock_service_client.return_value.get
    response = mock.Mock()
    response.status = 500
    mock_get.return_value.execute.side_effect = errors.HttpError(response, b'')

    with self.assertRaises(cloud_auth.Error):
      cloud_auth.get_service_account(self.project_id, self.service_account_name)

  def test_create_service_account_key(self):
    """Test that service account key is created and written to file object."""

    private_key = '{"key": "PRIVATE_KEY"}'
    private_key_encoded = base64.b64encode(private_key.encode())
    response = {'privateKeyData': private_key_encoded}
    mock_keys = self.mock_service_client.return_value.keys
    mock_keys.return_value.create.return_value.execute.return_value = response
    file_object = io.StringIO()

    cloud_auth._create_service_account_key(self.project_id,
                                           self.service_account_name,
                                           file_object)

    self.assertEqual(file_object.getvalue(), private_key)

  def test_service_account_email(self):
    """Test that service account email is returned."""

    expected_name = '{}@{}.iam.gserviceaccount.com'.format(
        self.service_account_name, self.project_id)

    name = cloud_auth._get_service_account_email(self.project_id,
                                                 self.service_account_name)

    self.assertEqual(name, expected_name)

  def test_fully_qualifed_service_account_name(self):
    service_account_email = cloud_auth._get_service_account_email(
        self.project_id, self.service_account_name)
    expected_fully_qualifed_name = 'projects/{}/serviceAccounts/{}'.format(
        self.project_id, service_account_email)

    actual_fully_qualifed_name = cloud_auth._get_service_account_name(
        self.project_id, self.service_account_name)

    self.assertEqual(expected_fully_qualifed_name, actual_fully_qualifed_name)

  @mock.patch.object(cloud_auth, '_get_resource_manager_client', autospec=True)
  def test_set_service_account_role(self, get_resource_manager_client):
    """Test that role is added to service account."""

    policy = {
        'version': 1,
        'etag': 'AABBCC',
        'bindings': [{
            'role': 'roles/owner',
            'members': ['abc@example.com']
        }]
    }
    manage_projects_client = get_resource_manager_client.return_value.projects
    (manage_projects_client.return_value.getIamPolicy.return_value.execute
     .return_value) = policy

    cloud_auth.set_service_account_role(self.project_id,
                                        self.service_account_name,
                                        self.role_name)

    expected_policy = {
        'version':
            1,
        'etag':
            'AABBCC',
        'bindings': [{
            'role': 'roles/owner',
            'members': ['abc@example.com'],
        }, {
            'role':
                f'roles/{self.role_name}',
            'members': [
                'serviceAccount:' + cloud_auth._get_service_account_email(
                    self.project_id, self.service_account_name)
            ],
        }]
    }
    manage_projects_client.return_value.setIamPolicy.assert_called_once_with(
        body={'policy': expected_policy}, resource=self.project_id)

  @mock.patch.object(os.path, 'isfile', autospec=True)
  @mock.patch.object(service_account.Credentials, 'from_service_account_file')
  def test_get_auth_session(self, mock_from_service_account_file, mock_is_file):
    mock_is_file.return_value = True
    mock_from_service_account_file.return_value = self.mock_credentials

    session = cloud_auth.get_auth_session('/tmp/valid_file')
    self.assertEqual(self.mock_credentials, session.credentials)

  def test_impersonate_service_account(self):
    self.mock_auth_default.return_value = (self.mock_credentials,
                                           self.project_id)

    credentials = cloud_auth.impersonate_service_account(
        self.service_account_name)

    self.assertIsNotNone(credentials)
    self.assertIsInstance(credentials, impersonated_credentials.Credentials)
    self.mock_auth_default.assert_called_once()

  @mock.patch.object(impersonated_credentials, 'Credentials', autospec=True)
  def test_impersonate_service_account_sets_target_scopes(
      self, mock_impersonated_credentials):
    target_scopes = ['https://www.googleapis.com/auth/devstorage.read_only']

    cloud_auth.impersonate_service_account(self.service_account_name,
                                           target_scopes)

    self.mock_auth_default.assert_called_once()
    mock_impersonated_credentials.assert_called_once_with(
        source_credentials=self.mock_credentials,
        target_principal=self.service_account_name,
        target_scopes=target_scopes)

  @mock.patch.object(cloud_auth, 'impersonate_service_account', autospec=True)
  def test_build_impersonated_client(self, mock_impersonate_service_account):
    service_name = 'service_name'
    version = 'v2'
    target_scopes = ['https://www.googleapis.com/auth/devstorage.read_only']
    mock_impersonate_service_account.return_value = self.mock_credentials

    cloud_auth.build_impersonated_client(
        service_name,
        self.service_account_name,
        version=version,
        target_scopes=target_scopes)

    self.mock_client.assert_called_once_with(
        service_name,
        version,
        credentials=self.mock_credentials,
        cache_discovery=False)


if __name__ == '__main__':
  absltest.main()
