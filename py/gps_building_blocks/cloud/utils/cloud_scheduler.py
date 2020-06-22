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

import dataclasses
import logging
from googleapiclient import errors

from gps_building_blocks.cloud.utils import cloud_auth

# Default Cloud Scheduler configuration.
_LOCATION = 'us-central1'
_CLIENT_NAME = 'cloudscheduler'
_VERSION = 'v1beta1'
_TIMEZONE = 'GMT'


@dataclasses.dataclass
class AppEngineTarget:
  http_method: str
  relative_uri: str
  service: str


class Error(Exception):
  """A generic error thrown for any exception in cloud_scheduler module."""
  pass


class CloudSchedulerUtils:
  """CloudSchedulerUtils class provides methods to manage Cloud Scheduler.

  This class manages Cloud Scheduler service within a single GCP project.

  Typical usage example:
       >>> scheduler = CloudSchedulerUtils(
             'project_id',
             'us-west1',
             'my-svc-account@project-id.iam.gserviceaccount.com')
       >>> scheduler.create_appengine_http_get_job(
             'cron_schedule',
             'appengine_relative_uri',
             '* * * * 1',
             appengine_target_instance)
  """

  def __init__(self,
               project_id: str,
               location: str = _LOCATION,
               service_account_name: Optional[str] = None,
               service_account_key_file: Optional[str] = None,
               version: str = _VERSION):
    """Initializes new instance of CloudSchedulerUtils.

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
      self._client = cloud_auth.build_service_client(_CLIENT_NAME,
                                                    service_account_key_file)
    else:
      self._client = cloud_auth.build_impersonated_client(
          _CLIENT_NAME, service_account_name, version)
    self._parent = f'projects/{project_id}/locations/{location}'

  def create_appengine_http_job(self,
                                name: str,
                                description: str,
                                schedule: str,
                                target: AppEngineTarget,
                                timezone: Optional[str] = _TIMEZONE) -> str:
    """Creates a new AppEngine HTTP job.

    Args:
      name: The name of the job.
      description: The description of the job.
      schedule: A cron-style schedule string.
      target: An AppEngineTarget instance containing the job target information.
      timezone: The timezone where of the job.

    Returns:
      The job name.

    Raises:
      Error: If the request was not processed successfully.
    """
    request_body = {
        'name': name,
        'description': description,
        'schedule': schedule,
        'timeZone': timezone,
        'appEngineHttpTarget': {
            'httpMethod': target.http_method,
            'appEngineRouting': {
                'service': target.service
            },
            'relativeUri': target.relative_uri
        }
    }

    try:
      job = self._client.projects().locations().jobs().create(
          parent=self._parent,
          body=request_body)
      result = job.execute()
      return result.name
    except errors.HttpError as error:
      logging.exception('Error occurred while creating job: %s', error)
      raise Error(f'Error occurred while creating job: {error}')


