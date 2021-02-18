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

"""Test for gps_building_blocks.tcrm.cloud_env_setup."""

import argparse
import os
import unittest

import mock

from gps_building_blocks.cloud.utils import cloud_api
from gps_building_blocks.cloud.utils import cloud_auth
from gps_building_blocks.cloud.utils import cloud_composer
from gps_building_blocks.cloud.utils import cloud_storage
from gps_building_blocks.tcrm import cloud_env_setup

_GCS_DAGS_FOLDER = 'gs://bucket_name/dags'


class CloudEnvSetupTest(unittest.TestCase):

  def setUp(self):
    super(CloudEnvSetupTest, self).setUp()
    self.addCleanup(mock.patch.stopall)
    self.mock_parse_args = mock.patch.object(
        cloud_env_setup, 'parse_arguments', autospec=True).start()
    self.service_account_key_file = 'service_account_key.json'
    self.project_id = 'project_id'
    self.mock_parse_args.return_value = argparse.Namespace(
        project_id=self.project_id,
        service_account_key_file=self.service_account_key_file,
        composer_env_name=cloud_env_setup._COMPOSER_ENV_NAME,
        local_dags_folder=cloud_env_setup._LOCAL_DAGS_FOLDER)

    # Setup mocks for cloud utils.
    self.mock_create_service_account = mock.patch.object(
        cloud_auth, 'create_service_account', autospec=True).start()
    self.mock_cloud_api_utils = mock.patch.object(
        cloud_api, 'CloudApiUtils', autospec=True).start()
    self.mock_cloud_composer_utils = mock.patch.object(
        cloud_composer, 'CloudComposerUtils', autospec=True).start()
    self.mock_cloud_storage_utils = mock.patch.object(
        cloud_storage, 'CloudStorageUtils', autospec=True).start()
    self.mock_composer_return_value = (
        self.mock_cloud_composer_utils.return_value)
    self.mock_cloud_composer_utils.return_value.get_dags_folder.return_value = (
        _GCS_DAGS_FOLDER)

  def test_service_account_is_created(self):
    cloud_env_setup.main()

    self.mock_create_service_account.assert_called_once_with(
        self.project_id, cloud_env_setup._SERVICE_ACCOUNT_NAME,
        cloud_env_setup._SERVICE_ACCOUNT_ROLE, self.service_account_key_file)

  def test_apis_are_enabled(self):
    mock_enable_apis = self.mock_cloud_api_utils.return_value.enable_apis

    cloud_env_setup.main()

    self.mock_cloud_api_utils.assert_called_once_with(
        project_id=self.project_id,
        service_account_key_file=self.service_account_key_file)
    mock_enable_apis.assert_called_once_with(
        cloud_env_setup._APIS_TO_BE_ENABLED)

  def test_composer_env_is_created(self):
    mock_create_environment = self.mock_composer_return_value.create_environment

    cloud_env_setup.main()

    self.mock_cloud_composer_utils.assert_called_once_with(
        project_id=self.project_id,
        service_account_key_file=self.service_account_key_file)
    mock_create_environment.assert_called_once_with(
        cloud_env_setup._COMPOSER_ENV_NAME)

  def test_composer_py_packages_are_installed(self):
    mock_install_packages = (
        self.mock_composer_return_value.install_python_packages)

    cloud_env_setup.main()

    mock_install_packages.assert_called_once_with(
        cloud_env_setup._COMPOSER_ENV_NAME,
        cloud_env_setup._COMPOSER_PYPI_PACKAGES)

  def test_composer_env_variables_are_set(self):
    mock_set_env_variables = (
        self.mock_composer_return_value.set_environment_variables)

    cloud_env_setup.main()

    mock_set_env_variables.assert_called_once_with(
        cloud_env_setup._COMPOSER_ENV_NAME,
        cloud_env_setup._COMPOSER_ENV_VARIABLES)

  def test_dags_and_plugincs_are_copied_to_composer_env_dag_folder(self):
    mock_upload_directory = (
        self.mock_cloud_storage_utils.return_value.upload_directory_to_url)

    cloud_env_setup.main()

    self.mock_cloud_storage_utils.assert_called_once_with(
        project_id=self.project_id,
        service_account_key_file=self.service_account_key_file)
    mock_upload_directory.assert_called_once_with(
        cloud_env_setup._LOCAL_DAGS_FOLDER, os.path.dirname(_GCS_DAGS_FOLDER))


if __name__ == '__main__':
  unittest.main()
