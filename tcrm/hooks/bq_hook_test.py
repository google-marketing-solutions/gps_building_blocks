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

"""Tests for gps_building_blocks.tcrm.hooks.bq_hook."""

import time
from typing import Any, Dict, List, Text
import unittest
from unittest import mock

from googleapiclient import errors as googleapiclient_errors

from gps_building_blocks.tcrm.hooks import bq_hook
from gps_building_blocks.tcrm.utils import errors


MOCK_BQ_HOOK = ('airflow.contrib.hooks.bigquery_hook.BigQueryHook.__init__')
_TIME_ANCHOR = time.time()


class FakeDataGenerator(object):

  def __init__(self, expected: List[Dict[Text, Any]]):
    self.expected = expected
    self.total_rows = len(expected)
    self.fields = self._generate_fields()

  def _generate_fields(self):
    fields = []
    if self.total_rows > 0:
      for key, value in self.expected[0].items():
        if isinstance(value, str):
          col_type = 'STRING'
        elif isinstance(value, bool):
          col_type = 'BOOLEAN'
        elif isinstance(value, int):
          col_type = 'INTEGER'
        elif isinstance(value, float) and value > _TIME_ANCHOR:
          col_type = 'TIMESTAMP'
        elif isinstance(value, float):
          col_type = 'FLOAT'
        fields.append({'name': key, 'type': col_type})
    return fields

  def get_data(self, start, length):
    if self.total_rows < start + length:
      raise IndexError()

    rows = []
    for idx in range(start, start + length):
      values = []
      item = self.expected[idx]
      for key in item:
        values.append({'v': str(item[key])})
      rows.append({'f': values})

    data = {
        'totalRows': self.total_rows,
        'schema': {
            'fields': self.fields
        },
        'rows': rows
    }
    return data


def fake_table_generator(expected: List[Text]):
  """Generates BigQuery table metadata in required format.

  For more information refer to
  https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/list

  Args:
    expected: A list of BigQuery table id.

  Yields:
    A single page of BigQuery table id list.
  """
  if not expected or bq_hook._DEFAULT_PAGE_SIZE <= 0:
    return

  count = 0
  tables = []
  length = len(expected)

  for idx, item in enumerate(expected):
    count = count + 1
    tables.append(
        {
            'tableReference': {
                'tableId': item
            }
        })
    if count == bq_hook._DEFAULT_PAGE_SIZE:
      if idx == length - 1:
        next_page_token = None
      else:
        next_page_token = 'token'
      next_data = {
          'totalItems': length,
          'nextPageToken': next_page_token,
          'tables': tables
      }
      yield next_data
      tables = []
      count = 0

  if tables:
    next_data = {
        'totalItems': length,
        'nextPageToken': None,
        'tables': tables
    }
    yield next_data


class MockedBigQueryCursor():
  """Replacement Mock for BigQueryCursor."""

  def __init__(self, data_generator=None, table_generator=None, fields=None):
    self.project_id = 'test_project'
    self.data_generator = data_generator
    self.table_generator = table_generator
    self.fields = fields
    if table_generator:
      self.service = mock.MagicMock()
      self.service.tables = mock.MagicMock()
      self.service.tables.return_value = mock.MagicMock()
      self.service.tables().list = mock.MagicMock()
      self.service.tables().list.return_value = mock.MagicMock()
      self.service.tables().list().execute = self.service_tables_list

  def service_tables_list(self):
    """Mock function of BigQueryCursor.service.tables().list().execute()."""
    try:
      return next(self.table_generator)
    except StopIteration:
      return None

  def get_tabledata(self, dataset_id, table_id, max_results, page_token=None,
                    start_index=None, selected_fields=None, return_none=False):
    """Mock method of BigQueryCursor.get_tabledata()."""
    if return_none:
      return None

    self.dataset_id = dataset_id
    self.table_id = table_id
    self.max_results = max_results
    self.page_token = page_token
    self.start_index = start_index
    self.selected_fields = selected_fields

    if table_id.startswith('error') and start_index == 1:
      response = mock.Mock()
      response.reason = 'test_reason'
      raise googleapiclient_errors.HttpError(
          resp=response, content=b'test')

    try:
      return self.data_generator.get_data(start_index, max_results)
    except IndexError:
      return None

  def get_schema(self, dataset_id, table_id):
    """Mock method of BigQueryCursor.get_schema()."""
    self.dataset_id = dataset_id
    self.table_id = table_id
    return {'fields': self.fields}


class BigqueryHookTest(unittest.TestCase):

  @mock.patch(MOCK_BQ_HOOK)
  def setUp(self, mocked_hook):
    super(BigqueryHookTest, self).setUp()

    self.project_id = 'test_project'
    self.dataset_id = 'test_dataset'
    self.table_id = 'test_table'

    mocked_hook.return_value = mock.MagicMock(
        bigquery_conn_id='test_conn', autospec=True)
    bq_hook.BigQueryHook._get_field = mock.MagicMock()
    bq_hook.BigQueryHook._get_field.return_value = self.project_id

    self.hook = bq_hook.BigQueryHook(bq_conn_id='test_conn',
                                     bq_dataset_id=self.dataset_id,
                                     bq_table_id=self.table_id)
    self.hook.get_conn = mock.MagicMock()
    self.hook.get_conn.return_value = mock.MagicMock()
    self.hook.get_conn.cursor = mock.MagicMock()

    self.error_hook = bq_hook.BigQueryHook(bq_conn_id='test_conn',
                                           bq_dataset_id='test_dataset',
                                           bq_table_id='error_table',)
    self.error_hook.get_conn = mock.MagicMock()
    self.error_hook.get_conn.return_value = mock.MagicMock()
    self.error_hook.get_conn.cursor = mock.MagicMock()

    bq_hook._DEFAULT_PAGE_SIZE = 1
    self.fields = [{'name': 'a', 'type': 'STRING'},
                   {'name': 'b', 'type': 'STRING'}]

  def test_events_blobs_generator_get_expected_output(self):
    expected = [{
        'a': '1',
        'b': '2'
    }, {
        'a': '3',
        'b': '4'
    }]
    self.hook.get_conn().cursor.return_value = MockedBigQueryCursor(
        data_generator=FakeDataGenerator(expected), fields=self.fields)

    result_list = []
    for blob_item in self.hook.events_blobs_generator():
      result_list.extend(blob_item.events)

    self.assertEqual(
        self.hook.url,
        'bq://{}.{}.{}'.format('test_project', 'test_dataset', 'test_table'))
    self.assertListEqual(expected, result_list)

  def test_events_blobs_generator_with_all_data_types(self):
    bq_hook._DEFAULT_PAGE_SIZE = 30
    expected = [{
        'a': 2,
        'b': 'text',
        'c': 1.1,
        'd': time.time(),
        'e': True
    }]
    fields = [{'name': 'a', 'type': 'INTEGER'},
              {'name': 'b', 'type': 'STRING'},
              {'name': 'c', 'type': 'FLOAT'},
              {'name': 'd', 'type': 'TIMESTAMP'},
              {'name': 'e', 'type': 'BOOLEAN'}]
    self.hook.get_conn().cursor.return_value = MockedBigQueryCursor(
        data_generator=FakeDataGenerator(expected), fields=fields)

    result_list = []
    for blob_item in self.hook.events_blobs_generator():
      result_list.extend(blob_item.events)

    self.assertListEqual(expected, result_list)

  def test_events_blobs_generator_with_larger_data(self):
    bq_hook._DEFAULT_PAGE_SIZE = 30
    expected = [{
        'a': 2,
        'b': 'c',
        'c': 'a'
    }]
    fields = [{'name': 'a', 'type': 'INTEGER'},
              {'name': 'b', 'type': 'STRING'},
              {'name': 'c', 'type': 'STRING'}]
    expected = expected * 100
    self.hook.get_conn().cursor.return_value = MockedBigQueryCursor(
        data_generator=FakeDataGenerator(expected), fields=fields)

    result_list = []
    for blob_item in self.hook.events_blobs_generator():
      result_list.extend(blob_item.events)

    self.assertListEqual(expected, result_list)

  def test_events_blobs_generator_get_expected_blob_metadata(self):
    expected = [{
        'a': '1',
        'b': '2'
    }, {
        'a': '3',
        'b': '4'
    }]
    self.hook.get_conn().cursor.return_value = MockedBigQueryCursor(
        data_generator=FakeDataGenerator(expected), fields=self.fields)

    start_index = 0
    for blob_item in self.hook.events_blobs_generator():
      self.assertEqual(start_index, blob_item.position)
      start_index = start_index + bq_hook._DEFAULT_PAGE_SIZE

  def test_events_blobs_generator_get_no_result(self):
    expected = []
    self.hook.get_conn().cursor.return_value = MockedBigQueryCursor(
        data_generator=FakeDataGenerator(expected), fields=self.fields)

    result_list = []
    with self.assertRaises(errors.DataInConnectorError):
      for blob_item in self.hook.events_blobs_generator():
        result_list.extend(blob_item.events)

  def test_events_blobs_generator_get_result_with_invalid_total_rows(self):
    self.hook.get_conn().cursor.return_value = MockedBigQueryCursor(
        data_generator=FakeDataGenerator([]), fields=self.fields)

    result_list = []
    with self.assertRaises(errors.DataInConnectorError):
      for blob_item in self.hook.events_blobs_generator():
        result_list.extend(blob_item.events)

  def test_events_blobs_generator_get_error_when_processing(self):
    expected = [{
        'a': '1',
        'b': '2'
    }, {
        'a': '3',
        'b': '4'
    }, {
        'a': '5',
        'b': '6'
    }]
    self.error_hook.get_conn().cursor.return_value = MockedBigQueryCursor(
        data_generator=FakeDataGenerator(expected), fields=self.fields)

    result_list = []
    blob_list = []
    for blob_item in self.error_hook.events_blobs_generator():
      result_list.extend(blob_item.events if blob_item else ())
      blob_list.append(blob_item)

    self.assertEqual(
        self.error_hook.url,
        'bq://{}.{}.{}'.format('test_project', 'test_dataset', 'error_table'))
    del expected[1]
    self.assertListEqual(expected, result_list)

  def test_list_tables_get_expected_output(self):
    expected = ['table1', 'table2', 'table3']
    self.hook.get_conn().cursor.return_value = MockedBigQueryCursor(
        table_generator=fake_table_generator(expected))

    table_ids = self.hook.list_tables()

    self.assertListEqual(expected, table_ids)

  def test_list_tables_get_expected_output_with_prefix(self):
    expected = ['table1', 'table2']
    sample_table_ids = ['table1', 'table2', 'chair3']
    self.hook.get_conn().cursor.return_value = MockedBigQueryCursor(
        table_generator=fake_table_generator(sample_table_ids))

    table_ids = self.hook.list_tables(prefix='table')

    self.assertListEqual(expected, table_ids)

  def test_get_location(self):
    location = self.hook.get_location()
    self.assertEqual(
        location, f'bq://{self.project_id}.{self.dataset_id}.{self.table_id}')

  def test_events_blobs_generator_read_once(self):
    bq_hook._DEFAULT_PAGE_SIZE = 30
    expected = []
    fields = [{'name': 'a', 'type': 'INTEGER'},
              {'name': 'b', 'type': 'STRING'},
              {'name': 'c', 'type': 'STRING'}]
    for i in range(0, 100):
      expected.append({'a': i, 'b': 'b', 'c': 'c'})
    self.hook.get_conn().cursor.return_value = MockedBigQueryCursor(
        data_generator=FakeDataGenerator(expected), fields=fields)

    processed_ranges = [('10', '50'), ('80', '20')]
    range_generator = (
        processed_ranges[i] for i in range(0, len(processed_ranges)))

    result_list = []
    for blob_item in self.hook.events_blobs_generator(
        processed_blobs_generator=range_generator):
      result_list.extend(blob_item.events)

    expected_read_list = expected[0:10] + expected[60:80]
    self.assertListEqual(expected_read_list, result_list)


if __name__ == '__main__':
  unittest.main()
