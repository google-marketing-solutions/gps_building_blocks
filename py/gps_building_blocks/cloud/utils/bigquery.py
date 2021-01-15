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
import re
from typing import Any, Optional, Sequence

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
       >>> bigquery.run_query('SELECT * FROM `my_project.my_dataset.my_table`')
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
        service accounts - 1) Use `build_service_client` method from
        `cloud_auth` module. 2) Use `gcloud` command line utility as documented
        here -
               https://cloud.google.com/iam/docs/creating-managing-service-account-keys
    """
    if service_account_name:
      credentials = cloud_auth.impersonate_service_account(service_account_name)
    elif service_account_key_file:
      credentials = cloud_auth.get_credentials(service_account_key_file)
    else:
      logging.info('Neither Service account key file nor service account '
                   'name was provided, so using default credentials.')
      credentials = cloud_auth.get_default_credentials()

    self.client = bigquery.Client(project=project_id, credentials=credentials)

  def run_query(self, query: str) -> bigquery.QueryJob:
    """Performs a SQL query.

    Args:
      query: The SQL query statement.

    Returns:
      google.cloud.bigquery.QueryJob: The query object.
    """
    return self.client.query(query)

  def insert_rows(self, table: str, rows: Sequence[Any]) -> Sequence[Any]:
    """Inserts rows into a table.

    Args:
      table: The full table name.
      rows: Sequence of rows that must be inserted.

    Returns:
      One mapping per row with insert errors: the “index” key identifies the
      row, and the “errors” key contains a list of the mappings describing one
      or more problems with the row. If the sequence is empty, it means all rows
      were inserted successfully.

    Raises:
      ValueError: If the table name is invalid or rows are empty.
    """
    if re.search(r'[\d\-]|[^\w.]', table):
      raise ValueError('Invalid table name')

    if not rows:
      raise ValueError('There are no rows to be inserted.')

    return self.client.insert_rows(table, rows)
