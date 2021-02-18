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

"""Cloud Environment setup module for TCRM.

This module automates the following 4 steps:
  1. Enable all the required Cloud APIs.
  2. Create and update the Cloud Composer environment.
  3. Install all the required Python packages.
  4. Move TCRM plugins from Cloud Console local environment to Cloud Storage
  bucket.
"""

import argparse
import logging
import os

from gps_building_blocks.cloud.utils import cloud_api
from gps_building_blocks.cloud.utils import cloud_auth
from gps_building_blocks.cloud.utils import cloud_composer
from gps_building_blocks.cloud.utils import cloud_storage

_COMPOSER_ENV_NAME = 'tcrm-env'

# Required Cloud APIs to be enabled.
_APIS_TO_BE_ENABLED = [
    'bigquery-json.googleapis.com', 'cloudapis.googleapis.com',
    'composer.googleapis.com', 'googleads.googleapis.com',
    'storage-api.googleapis.com'
]
# Required Python packages.
_COMPOSER_PYPI_PACKAGES = {
    'dataclasses': '',
    'googleads': '',
    'frozendict': '',
}
# Composer environment variables.
_COMPOSER_ENV_VARIABLES = {'PYTHONPATH': '/home/airflow/gcs'}

# Local folder names.
_LOCAL_DAGS_FOLDER = 'src/'

# Service account constants.
_SERVICE_ACCOUNT_KEY_FILE = '/tmp/service_account_key.json'
_SERVICE_ACCOUNT_NAME = 'tcrm-sa'
_SERVICE_ACCOUNT_ROLE = 'editor'


def parse_arguments() -> argparse.Namespace:
  """Initialize command line parser using argparse.

  Returns:
    An argparse.ArgumentParser.
  """
  parser = argparse.ArgumentParser()
  parser.add_argument('--project_id', help='GCP project id.', required=True)
  parser.add_argument(
      '--service_account_key_file',
      help='Path to GCP service account file.',
      default=_SERVICE_ACCOUNT_KEY_FILE,
      required=False)
  parser.add_argument(
      '--composer_env_name',
      help='GCP cloud composer environment name.',
      default=_COMPOSER_ENV_NAME,
      required=False)
  parser.add_argument(
      '--local_dags_folder',
      help='Path of the DAGs folder.',
      default=_LOCAL_DAGS_FOLDER,
      required=False)

  return parser.parse_args()


def main() -> None:
  logging.getLogger('').setLevel(logging.INFO)
  args = parse_arguments()

  # Create service account.
  cloud_auth.create_service_account(
      project_id=args.project_id,
      service_account_name=_SERVICE_ACCOUNT_NAME,
      role_name=_SERVICE_ACCOUNT_ROLE,
      file_name=args.service_account_key_file)

  # Initialize cloud util classes.
  cloud_api_utils = cloud_api.CloudApiUtils(
      project_id=args.project_id,
      service_account_key_file=args.service_account_key_file)
  cloud_composer_utils = cloud_composer.CloudComposerUtils(
      project_id=args.project_id,
      service_account_key_file=args.service_account_key_file)
  cloud_storage_utils = cloud_storage.CloudStorageUtils(
      project_id=args.project_id,
      service_account_key_file=args.service_account_key_file)

  # Enable required Cloud APIs.
  cloud_api_utils.enable_apis(apis=_APIS_TO_BE_ENABLED)

  # Create Cloud Composer environment.
  cloud_composer_utils.create_environment(
      environment_name=args.composer_env_name)

  # Set Cloud Composer environment variables.
  cloud_composer_utils.set_environment_variables(
      environment_name=args.composer_env_name,
      environment_variables=_COMPOSER_ENV_VARIABLES)

  # Copy local DAGs and dependencies to Cloud Storage dag and plugins folders.
  dags_folder_url = cloud_composer_utils.get_dags_folder(
      environment_name=args.composer_env_name)
  gcs_dags_path = os.path.dirname(dags_folder_url)
  cloud_storage_utils.upload_directory_to_url(
      source_directory_path=args.local_dags_folder,
      destination_dir_url=gcs_dags_path)

  # Install required Python packages on Cloud Composer environment.
  cloud_composer_utils.install_python_packages(
      environment_name=args.composer_env_name, packages=_COMPOSER_PYPI_PACKAGES)


if __name__ == '__main__':
  main()
