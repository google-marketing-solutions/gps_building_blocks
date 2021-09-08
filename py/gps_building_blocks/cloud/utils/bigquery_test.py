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
"""Tests for gps_building_blocks.cloud.utils.bigquery."""
from google.api_core import exceptions
from google.auth import credentials
from google.cloud import bigquery

from absl.testing import absltest
from absl.testing import parameterized
from absl.testing.absltest import mock
from gps_building_blocks.cloud.utils import bigquery as bigquery_utils
from gps_building_blocks.cloud.utils import cloud_auth


class BigQueryTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.addCleanup(mock.patch.stopall)
    self.project_id = 'project-id'
    # Mock for google.cloud.bigquery.Client object
    self.mock_client = mock.patch.object(
        bigquery, 'Client', autospec=True).start()
    self.mock_dataset = mock.patch.object(
        bigquery, 'Dataset', autospec=True).start()
    self.mock_dataset_ref = mock.patch.object(
        bigquery, 'DatasetReference', autospec=True).start()
    self.mock_get_credentials = mock.patch.object(
        cloud_auth, 'get_default_credentials', autospec=True).start()
    self.mock_credentials = mock.Mock(credentials.Credentials, autospec=True)
    self.mock_get_credentials.return_value = self.mock_credentials
    self.service_account_name = (
        'my-svc-account@project-id.iam.gserviceaccount.com')
    self.bigquery_client = bigquery_utils.BigQueryUtils(self.project_id)

  @mock.patch.object(cloud_auth, 'impersonate_service_account', autospec=True)
  def test_client_initializes_with_impersonated_service_account(
      self, mock_impersonated_account):
    mock_impersonated_account.return_value = self.mock_credentials

    bigquery_utils.BigQueryUtils(
        project_id=self.project_id,
        service_account_name=self.service_account_name)

    mock_impersonated_account.assert_called_once_with(self.service_account_name)
    self.mock_client.assert_called_with(
        project=self.project_id, credentials=self.mock_credentials)

  def test_run_query(self):
    sql_statement = 'SELECT * FROM `my_project.my_dataset.my_table`'
    self.bigquery_client.run_query(sql_statement)
    self.mock_client.return_value.query.assert_called_once_with(sql_statement)

  def test_insert_rows_valid_parameters(self):
    table_name = 'my_project.my_dataset.my_table'
    rows = [{'name': 'Foo', 'age': 10}, {'name': 'Bar', 'age': 15}]
    self.bigquery_client.insert_rows(table_name, rows)
    self.mock_client.return_value.insert_rows.assert_called_once_with(
        table_name, rows)

  @parameterized.parameters(
      'my_project.my-data-set.my-table',
      'my_project.mydataset01.mytable01',
      'my_project.my$data!set.my&*table',
      'my_project.my-dataset.my$-table01')
  def test_insert_rows_invalid_table_name(self, table_name):
    with self.assertRaises(ValueError):
      self.bigquery_client.insert_rows(table_name, [{'name': 'Foo'}])

  def test_insert_rows_empty_row(self):
    with self.assertRaises(ValueError):
      table_name = 'project.dataset.table'
      self.bigquery_client.insert_rows(table_name, [])

  def test_create_dataset_creates_new_dataset_in_the_project(self):
    dataset_name = 'test_dataset'
    expected_arg = bigquery.Dataset(
        bigquery.DatasetReference(self.project_id, dataset_name))

    self.bigquery_client.create_dataset(dataset_name)
    self.mock_client.return_value.create_dataset.assert_called_once_with(
        expected_arg, timeout=30)

  def test_create_dataset_raises_valueerror_if_dataset_exists(self):
    dataset_name = 'test_dataset'
    self.mock_client.return_value.create_dataset.side_effect = [
        exceptions.Conflict('Dataset %s exists.', dataset_name)
    ]
    with self.assertRaises(ValueError):
      self.bigquery_client.create_dataset(dataset_name, fail_if_exists=True)


if __name__ == '__main__':
  absltest.main()
