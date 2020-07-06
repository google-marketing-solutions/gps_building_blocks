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
"""Manage operations on BigQuery."""
import logging
from typing import Optional

from google.cloud import bigquery
from gps_building_blocks.cloud.utils import cloud_auth


class BigQueryUtils:
  """BigQueryUtils class provides methods to manage BigQuery.

  This class manages BigQuery queries within a single GCP project.

  Typical usage example:
       >>> bigquery = BigQueryUtils(
             'project_id',
             'us-west1',
             'my-svc-account@project-id.iam.gserviceaccount.com')
  """

  def __init__(self,
               project_id: str,
               service_account_name: Optional[str] = None,
               service_account_key_file: Optional[str] = None) -> None:
    """Initialize new instance of BigQueryUtils.

    Args:
      project_id: GCP project id.
      service_account_name: The service account name.
      service_account_key_file: File containing service account key. If both
        service_account_name and service_account_key_file are not passed the
        default credential will be used.There are following ways to create
        service accounts -
          1) Use `build_service_client` method from `cloud_auth` module.
          2) Use `gcloud` command line utility as documented here -
               https://cloud.google.com/iam/docs/creating-managing-service-account-keys
    """
    if service_account_name:
      credentials = cloud_auth.impersonate_service_account(service_account_name)
    else:
      if not service_account_key_file:
        logging.info('Neither Service account key file nor servie account name '
                     'was provided. So using default credentials.')
      credentials = cloud_auth.get_credentials(service_account_key_file)
    self.client = bigquery.Client(project=project_id, credentials=credentials)
