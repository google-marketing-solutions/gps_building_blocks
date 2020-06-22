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

"""Manage operations on Cloud Scheduler."""

from typing import Optional

from gps_building_blocks.cloud.utils import cloud_auth

# Default Cloud Scheduler configuration.
_LOCATION = 'us-central1'
_CLIENT_NAME = 'cloudscheduler'
_VERSION = 'v1beta1'


class CloudSchedulerUtils:
  """CloudSchedulerUtils class provides methods to manage Cloud Scheduler.

  This class manages Cloud Scheduler service within a single GCP project.

  Typical usage example:
       >>> scheduler = CloudSchedulerUtils('project_id',
                                           'us-west1',
                                           'service_account_key_file.json')
       >>> scheduler.create_appengine_http_get_job('cron_schedule',
                                                   'appengine_relative_ui')
  """

  def __init__(self,
               project_id: str,
               location: str = _LOCATION,
               service_account_name: Optional[str] = None,
               service_account_key_file: Optional[str] = None,
               version: str = _VERSION):
    """Initialize new instance of CloudSchedulerUtils.

    Args:
      project_id: GCP project id.
      location: Optional. Region under which the Cloud Scheduler needs to be
      managed. It defaults to 'us-central1'. Allowed values -
        https://cloud.google.com/compute/docs/regions-zones/.
      service_account_name: The service account name.
      service_account_key_file: Optional. File containing service account key.
        If not passed the default credential will be used. There are following
        ways to create service accounts: 1. Use `build_service_client` method
          from `cloud_auth` module. 2. Use `gcloud` command line utility as
          documented here -
             https://cloud.google.com/iam/docs/creating-managing-service-account-keys
      version: The version of the service. It defaults to 'v1beta1'.

    Raises:
      ValueError: If neither service_account_key_file or service_account_name
        were provided.
    """
    if not service_account_key_file and not service_account_name:
      raise ValueError(
          'Service account key file or service account name is not provided. '
          'Provide either path to service account key file or name of the '
          'service account.')

    if service_account_key_file:
      self.client = cloud_auth.build_service_client(_CLIENT_NAME,
                                                    service_account_key_file)
    else:
      self.client = cloud_auth.build_impersonated_client(
          _CLIENT_NAME, service_account_name, version)
    self.project_id = project_id
    self.location = location
