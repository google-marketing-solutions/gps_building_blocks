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
"""Tests for gps_building_blocks.cloud.utils.cloud_composer."""

from typing import Dict

from google.auth import credentials
from googleapiclient import errors
from googleapiclient import http

from absl.testing import absltest
from absl.testing.absltest import mock
from gps_building_blocks.cloud.utils import cloud_auth
from gps_building_blocks.cloud.utils import cloud_composer
from gps_building_blocks.cloud.utils import utils


class CloudComposerUtilsTest(absltest.TestCase):

  def setUp(self):
    """Creates mock objects for googleapi client."""
    super(CloudComposerUtilsTest, self).setUp()
    self.addCleanup(mock.patch.stopall)
    self.project_id = 'project_id'
    self.location = cloud_composer._LOCATION
    self.environment_name = 'environment_name'
    self.zone = 'a'
    self.mock_get_credentials = mock.patch.object(
        cloud_auth, 'get_credentials', autospec=True).start()
    self.mock_build_service_client = mock.patch.object(
        cloud_auth, 'build_service_client', autospec=True).start()
    self.mock_wait_for_operation = mock.patch.object(
        utils, 'wait_for_operation', autospec=True).start()
    self.mock_execute_request = mock.patch.object(
        utils, 'execute_request', autospec=True).start()
    self.mock_client = mock.Mock()
    self.mock_credentials = mock.Mock(credentials.Credentials, autospec=True)
    self.mock_build_service_client.return_value = self.mock_client
    self.mock_get_credentials.return_value = self.mock_credentials

    self.service_account_key_file = '/tmp/service_account_key.json'
    self.composer = cloud_composer.CloudComposerUtils(
        project_id=self.project_id,
        location=self.location,
        service_account_key_file=self.service_account_key_file)
    self.operation_client = mock.Mock()
    self.operation = {}
    (self.mock_client.projects.return_value.locations.return_value.operations
     .return_value) = self.operation_client
    self.mock_execute_request.return_value = self.operation
    self.http_error = errors.HttpError(mock.MagicMock(status=400), b'')
    self.fully_qualified_name = (f'projects/{self.project_id}/locations/'
                                 f'{self.location}/environments/'
                                 f'{self.environment_name}')
    self.mock_environment_client = (
        self.mock_client.projects.return_value.locations.return_value
        .environments)
    self.mock_request = mock.Mock(http.HttpRequest)

  @mock.patch.object(cloud_auth, 'build_impersonated_client', autospec=True)
  def test_client_initializes_with_impersonated_service_account(
      self, mock_impersonated_client):
    service_account_name = 'my-svc-account@project-id.iam.gserviceaccount.com'
    version = 'v1beta1'

    cloud_composer.CloudComposerUtils(
        project_id=self.project_id,
        service_account_name=service_account_name,
        version=version)

    mock_impersonated_client.assert_called_once_with('composer',
                                                     service_account_name,
                                                     version)

  def test_get_fully_qualified_environment_name(self):
    expected_name = (f'projects/{self.project_id}/locations/'
                     f'{self.location}/environments/{self.environment_name}')

    fully_qualified_name = self.composer._get_fully_qualified_env_name(
        self.environment_name)

    self.assertEqual(fully_qualified_name, expected_name)

  def test_create_composer_environment(self):
    mock_create_env = self.mock_environment_client.return_value.create
    disk_size_gb = cloud_composer._DISC_SIZE
    machine_type = cloud_composer._MACHINE_TYPE
    python_version = cloud_composer._PYTHON_VERSION

    self.composer.create_environment(self.environment_name, self.zone,
                                     disk_size_gb, machine_type)

    composer_zone = '{}-{}'.format(self.location, self.zone)
    location = 'projects/{}/zones/{}'.format(self.project_id, composer_zone)
    parent = 'projects/{}/locations/{}'.format(self.project_id, self.location)
    machine_type = 'projects/{}/zones/{}/machineTypes/{}'.format(
        self.project_id, composer_zone, machine_type)
    request_body = {
        'name': self.fully_qualified_name,
        'config': {
            'nodeConfig': {
                'location': location,
                'machineType': machine_type,
                'diskSizeGb': disk_size_gb
            },
            'softwareConfig': {
                'pythonVersion': python_version
            }
        }
    }
    mock_create_env.assert_called_once_with(parent=parent, body=request_body)
    self.mock_build_service_client.assert_called_once_with(
        'composer', self.mock_credentials)
    self.assertEqual(self.mock_client, self.composer.client)
    self.mock_wait_for_operation.assert_called_once_with(
        self.operation_client, self.operation)

  def test_create_composer_environment_raises_error(self):
    self.mock_client.projects.side_effect = self.http_error

    with self.assertRaises(cloud_composer.Error):
      self.composer.create_environment(self.environment_name, self.zone)

  def test_create_environment_raises_error_if_disk_size_is_less_than_minimum(
      self):
    with self.assertRaises(cloud_composer.Error):
      self.composer.create_environment(self.environment_name, disk_size_gb=10)

  def test_create_environment_skips_if_already_exists(self):
    http_error_conflict = errors.HttpError(mock.MagicMock(status=409), b'')
    self.mock_client.projects.side_effect = http_error_conflict

    self.composer.create_environment(self.environment_name)

  def test_create_environment_with_specific_composer_image_version(self):
    image_version = 'composer-1.9.2-airflow-1.10.2'
    mock_create_env = self.mock_environment_client.return_value.create

    self.composer.create_environment(
        self.environment_name, image_version=image_version)

    _, mock_kwargs = mock_create_env.call_args
    self.assertEqual(
        mock_kwargs['body']['config']['softwareConfig']['imageVersion'],
        image_version)

  def test_install_python_packages(self):
    python_packages = {'lib1': '<=1.0.1', 'lib2': '==2.12.0', 'lib3': '>1.0.3'}
    mock_update_environment = self.mock_environment_client.return_value.patch
    mock_update_environment.return_value = self.mock_request

    self.composer.install_python_packages(self.environment_name,
                                          python_packages)

    request_body = {
        'name': self.fully_qualified_name,
        'config': {
            'softwareConfig': {
                'pypiPackages': python_packages
            }
        }
    }
    mock_update_environment.assert_called_once_with(
        name=self.fully_qualified_name,
        body=request_body,
        updateMask='config.softwareConfig.pypiPackages')
    self.mock_execute_request.assert_called_once_with(self.mock_request)
    self.mock_wait_for_operation.assert_called_once_with(
        self.operation_client, self.operation)

  def test_install_python_packages_raises_error_on_empty_packages(self):
    python_packages = []

    with self.assertRaises(cloud_composer.Error):
      self.composer.install_python_packages(self.environment_name,
                                            python_packages)

  def test_install_python_packages_raises_error_on_http_error(self):
    python_packages = {'lib1': '==1.0.1'}
    self.mock_client.projects.side_effect = self.http_error

    with self.assertRaises(cloud_composer.Error):
      self.composer.install_python_packages(self.environment_name,
                                            python_packages)

  def test_set_environment_variables(self):
    environment_variables = {'name1': 'value1', 'name2': 'value2'}

    mock_update_environment = self.mock_environment_client.return_value.patch
    mock_update_environment.return_value = self.mock_request

    self.composer.set_environment_variables(self.environment_name,
                                            environment_variables)

    request_body = {
        'name': self.fully_qualified_name,
        'config': {
            'softwareConfig': {
                'envVariables': environment_variables
            }
        }
    }
    mock_update_environment.assert_called_once_with(
        name=self.fully_qualified_name,
        body=request_body,
        updateMask='config.softwareConfig.envVariables')
    self.mock_execute_request.assert_called_once_with(self.mock_request)
    self.mock_wait_for_operation.assert_called_once_with(
        self.operation_client, self.operation)

  def test_set_environment_variables_raises_error_on_http_error(self):
    environment_variables = {'name1': 'value1', 'name2': 'value2'}
    self.mock_client.projects.side_effect = self.http_error

    with self.assertRaises(cloud_composer.Error):
      self.composer.set_environment_variables(self.environment_name,
                                              environment_variables)

  def test_override_airflow_configs(self):
    airflow_config_overrides = {
        'smtp-smtp_mail_from': 'no-reply@abc.com',
        'core-dags_are_paused_at_creation': 'True'
    }
    expected_request_body = {
        'name': self.fully_qualified_name,
        'config': {
            'softwareConfig': {
                'airflowConfigOverrides': airflow_config_overrides
            }
        }
    }
    mock_update_environment = self.mock_environment_client.return_value.patch
    mock_update_environment.return_value = self.mock_request

    self.composer.override_airflow_configs(self.environment_name,
                                           airflow_config_overrides)

    mock_update_environment.assert_called_once_with(
        name=self.fully_qualified_name,
        body=expected_request_body,
        updateMask='config.softwareConfig.airflowConfigOverrides')
    self.mock_execute_request.assert_called_once_with(self.mock_request)
    self.mock_wait_for_operation.assert_called_once_with(
        self.operation_client, self.operation)

  def test_override_airflow_configs_raises_error_on_http_error(self):
    airflow_config_overrides = {}
    self.mock_client.projects.side_effect = self.http_error

    with self.assertRaises(cloud_composer.Error):
      self.composer.override_airflow_configs(self.environment_name,
                                             airflow_config_overrides)

  def test_get_environment(self):
    mock_environment = mock.Mock(Dict[str, str])
    self.mock_environment_client.return_value.get.return_value = (
        self.mock_request)
    self.mock_execute_request.return_value = mock_environment

    composer_environment = self.composer.get_environment(self.environment_name)

    self.assertEqual(composer_environment, mock_environment)
    self.mock_execute_request.assert_called_once_with(self.mock_request)

  def test_get_environment_raises_error_on_http_error(self):
    self.mock_environment_client.side_effect = self.http_error

    with self.assertRaises(cloud_composer.Error):
      self.composer.get_environment(self.environment_name)

  def test_get_dags_folder(self):
    dags_folder = 'gs://bucket_name/dags'
    composer_env_details = {'config': {'dagGcsPrefix': dags_folder}}
    self.mock_environment_client.return_value.get.return_value = (
        self.mock_request)
    self.mock_execute_request.return_value = composer_env_details

    actual_dags_folder = self.composer.get_dags_folder(self.environment_name)

    self.assertEqual(actual_dags_folder, dags_folder)

  def test_delete_environment(self):
    mock_delete_env = self.mock_environment_client.return_value.delete

    self.composer.delete_environment(self.environment_name)

    fully_qualified_name = self.fully_qualified_name

    mock_delete_env.assert_called_once_with(name=fully_qualified_name)
    self.mock_build_service_client.assert_called_once_with(
        'composer', self.mock_credentials)
    self.assertEqual(self.mock_client, self.composer.client)
    self.mock_wait_for_operation.assert_called_once_with(
        self.operation_client, self.operation)

  def test_delete_environment_raises_error_on_http_error(self):
    self.mock_environment_client.side_effect = self.http_error

    with self.assertRaises(cloud_composer.Error):
      self.composer.delete_environment(self.environment_name)

  def test_delete_environment_sliently_skips_on_http_error(self):
    http_error_conflict = errors.HttpError(mock.MagicMock(status=404), b'')
    self.mock_client.projects.side_effect = http_error_conflict

    self.composer.delete_environment(self.environment_name)

if __name__ == '__main__':
  absltest.main()
