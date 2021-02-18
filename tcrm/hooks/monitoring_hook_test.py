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

import datetime
import json
import unittest
from unittest import mock

from airflow import exceptions
from airflow.contrib.hooks import bigquery_hook
import freezegun

from gps_building_blocks.tcrm.hooks import monitoring_hook
from gps_building_blocks.tcrm.utils import errors


@freezegun.freeze_time('2020-11-01')
class MonitoringHookTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.dag_name = 'dag'
    self.project_id = 'test_project'
    self.dataset_id = 'test_dataset'
    self.table_id = 'test_table'
    self.conn_id = 'test_conn'
    self.expected_run_row = {
        'dag_name': self.dag_name,
        'timestamp': '20201103180000',
        'type_id': monitoring_hook.MonitoringEntityMap.RUN.value,
        'location': 'https://input/resource',
        'position': '',
        'info': ''}
    self.expected_blob_row = {
        'dag_name': self.dag_name,
        'timestamp': '20201103180000',
        'type_id': monitoring_hook.MonitoringEntityMap.BLOB.value,
        'location': 'https://input/resource',
        'position': '3000',
        'info': '1500'}
    self.expected_event_row = {
        'dag_name': self.dag_name,
        'timestamp': '20201103180000',
        'type_id': 50,
        'location': 'https://input/resource',
        'position': '60',
        'info': json.dumps({'a': 1})}
    self.expected_retry_row = {
        'dag_name': self.dag_name,
        'timestamp': '20201103180000',
        'type_id': monitoring_hook.MonitoringEntityMap.RETRY.value,
        'location': 'https://input/resource',
        'position': '',
        'info': ''}

    self.mock_conn_obj = mock.MagicMock()
    self.mock_cursor_obj = mock.MagicMock()
    self.mock_cursor_obj.project_id = self.project_id
    self.mock_conn_obj.cursor = mock.MagicMock(
        return_value=self.mock_cursor_obj)
    self.mock_cursor_obj.create_empty_table = mock.MagicMock()
    self.mock_cursor_obj.create_empty_dataset = mock.MagicMock()
    self.mock_cursor_obj.insert_all = mock.MagicMock()

    self.original_get_conn = monitoring_hook.MonitoringHook.get_conn
    monitoring_hook.MonitoringHook.get_conn = mock.MagicMock(
        return_value=self.mock_conn_obj)

    self.original_bigquery_hook_init = bigquery_hook.BigQueryHook.__init__
    bigquery_hook.BigQueryHook.__init__ = mock.MagicMock()

    with mock.patch(
        'google3.third_party.gps_building_blocks.tcrm.hooks.monitoring_hook.'
        'MonitoringHook._create_monitoring_dataset_and_table_if_not_exist'):
      self.hook = monitoring_hook.MonitoringHook(
          bq_conn_id=self.conn_id,
          monitoring_dataset=self.dataset_id,
          monitoring_table=self.table_id)
      self.hook.get_conn = mock.MagicMock(return_value=self.mock_conn_obj)

  def tearDown(self):
    super().tearDown()
    bigquery_hook.BigQueryHook.__init__ = self.original_bigquery_hook_init
    monitoring_hook.MonitoringHook.get_conn = self.original_get_conn

  def test_init(self):
    self.mock_cursor_obj.get_dataset.side_effect = exceptions.AirflowException()
    monitoring_hook.MonitoringHook.table_exists = mock.MagicMock(
        return_value=True)

    monitoring_hook.MonitoringHook(
        bq_conn_id='test_conn',
        monitoring_dataset=self.dataset_id,
        monitoring_table=self.table_id)

    self.mock_cursor_obj.get_dataset.assert_called_with(
        project_id=self.project_id, dataset_id=self.dataset_id)
    monitoring_hook.MonitoringHook.table_exists.assert_called_with(
        project_id=self.project_id, dataset_id=self.dataset_id,
        table_id=self.table_id)

  def test_init_create_monitoring_dataset_and_table(self):
    self.mock_cursor_obj.get_dataset.side_effect = exceptions.AirflowException()
    monitoring_hook.MonitoringHook.table_exists = mock.MagicMock(
        return_value=False)

    monitoring_hook.MonitoringHook(
        bq_conn_id='test_conn',
        monitoring_dataset=self.dataset_id,
        monitoring_table=self.table_id)

    self.mock_cursor_obj.create_empty_table.assert_called_with(
        project_id=self.project_id, dataset_id=self.dataset_id,
        table_id=self.table_id,
        schema_fields=monitoring_hook._LOG_SCHEMA_FIELDS)
    self.mock_cursor_obj.create_empty_dataset.assert_called_with(
        project_id=self.project_id, dataset_id=self.dataset_id)
    self.mock_cursor_obj.get_dataset.assert_called_with(
        project_id=self.project_id, dataset_id=self.dataset_id)
    monitoring_hook.MonitoringHook.table_exists.assert_called_with(
        project_id=self.project_id, dataset_id=self.dataset_id,
        table_id=self.table_id)

  def test_init_handles_bigquery_create_empty_dataset_errors(self):
    self.mock_cursor_obj.get_dataset.side_effect = exceptions.AirflowException()
    self.mock_cursor_obj.create_empty_dataset.side_effect = (
        exceptions.AirflowException())

    with self.assertRaises(errors.MonitoringDatabaseError):
      monitoring_hook.MonitoringHook(
          bq_conn_id='test_conn',
          monitoring_dataset=self.dataset_id,
          monitoring_table=self.table_id)

  def test_init_handles_bigquery_create_empty_table_errors(self):
    self.mock_cursor_obj.create_empty_table.side_effect = (
        exceptions.AirflowException())
    monitoring_hook.MonitoringHook.table_exists = mock.MagicMock(
        return_value=False)

    with self.assertRaises(errors.MonitoringDatabaseError):
      monitoring_hook.MonitoringHook(
          bq_conn_id='test_conn',
          monitoring_dataset=self.dataset_id,
          monitoring_table=self.table_id)

  def test_get_location(self):
    location = self.hook.get_location()
    self.assertEqual(location,
                     (f'bq://{self.hook.project_id}.'
                      f'{self.hook.dataset_id}.{self.hook.table_id}'))

  def test_store_run(self):
    self.hook.store_run(dag_name=self.expected_run_row['dag_name'],
                        timestamp=self.expected_run_row['timestamp'],
                        location=self.expected_run_row['location'])

    self.mock_cursor_obj.insert_all.assert_called_once_with(
        project_id=self.project_id, dataset_id=self.dataset_id,
        table_id=self.table_id, rows=[{'json': self.expected_run_row}])

  def test_store_run_creates_timestamp_when_none_provided(self):
    self.expected_run_row['timestamp'] = (monitoring_hook.
                                          _generate_zone_aware_timestamp())

    self.hook.store_run(dag_name=self.expected_run_row['dag_name'],
                        location=self.expected_run_row['location'])

    self.mock_cursor_obj.insert_all.assert_called_once_with(
        project_id=self.project_id, dataset_id=self.dataset_id,
        table_id=self.table_id, rows=[{'json': self.expected_run_row}])

  def test_store_run_handles_storing_error(self):
    self.mock_cursor_obj.insert_all.side_effect = exceptions.AirflowException()
    self.expected_run_row['timestamp'] = (monitoring_hook.
                                          _generate_zone_aware_timestamp())

    with self.assertRaises(errors.MonitoringAppendLogError):
      self.hook.store_run(dag_name=self.expected_run_row['dag_name'],
                          location=self.expected_run_row['location'])

  def test_store_blob(self):
    self.hook.store_blob(dag_name=self.expected_blob_row['dag_name'],
                         timestamp=self.expected_blob_row['timestamp'],
                         location=self.expected_blob_row['location'],
                         position=self.expected_blob_row['position'],
                         num_rows=self.expected_blob_row['info'])

    self.mock_cursor_obj.insert_all.assert_called_once_with(
        project_id=self.project_id, dataset_id=self.dataset_id,
        table_id=self.table_id, rows=[{'json': self.expected_blob_row}])

  def test_store_blobs_creates_timestamp_when_none_provided(self):
    self.expected_blob_row['timestamp'] = (monitoring_hook.
                                           _generate_zone_aware_timestamp())

    self.hook.store_blob(dag_name=self.expected_blob_row['dag_name'],
                         location=self.expected_blob_row['location'],
                         position=self.expected_blob_row['position'],
                         num_rows=self.expected_blob_row['info'])

    self.mock_cursor_obj.insert_all.assert_called_once_with(
        project_id=self.project_id, dataset_id=self.dataset_id,
        table_id=self.table_id, rows=[{'json': self.expected_blob_row}])

  def test_store_blob_handles_storing_error(self):
    self.mock_cursor_obj.insert_all.side_effect = exceptions.AirflowException()
    self.expected_blob_row['timestamp'] = (monitoring_hook.
                                           _generate_zone_aware_timestamp())

    with self.assertRaises(errors.MonitoringAppendLogError):
      self.hook.store_blob(dag_name=self.expected_blob_row['dag_name'],
                           location=self.expected_blob_row['location'],
                           position=self.expected_blob_row['position'],
                           num_rows=self.expected_blob_row['info'])

  def test_store_events(self):
    expected_event = (self.expected_event_row['position'],
                      json.loads(self.expected_event_row['info']),
                      self.expected_event_row['type_id'])
    self.expected_event_row['info'] = json.dumps(expected_event[1])

    self.hook.store_events(dag_name=self.expected_event_row['dag_name'],
                           timestamp=self.expected_event_row['timestamp'],
                           location=self.expected_event_row['location'],
                           id_event_error_tuple_list=[expected_event])

    self.mock_cursor_obj.insert_all.assert_called_once_with(
        project_id=self.project_id, dataset_id=self.dataset_id,
        table_id=self.table_id, rows=[{'json': self.expected_event_row}])

  def test_store_events_creates_timestamp_when_none_provided(self):
    expected_event = (self.expected_event_row['position'],
                      json.loads(self.expected_event_row['info']),
                      self.expected_event_row['type_id'])
    self.expected_event_row['timestamp'] = (monitoring_hook.
                                            _generate_zone_aware_timestamp())

    self.hook.store_events(dag_name=self.expected_event_row['dag_name'],
                           location=self.expected_event_row['location'],
                           id_event_error_tuple_list=[expected_event])

    self.mock_cursor_obj.insert_all.assert_called_once_with(
        project_id=self.project_id, dataset_id=self.dataset_id,
        table_id=self.table_id, rows=[{'json': self.expected_event_row}])

  def test_store_events_handles_storing_error(self):
    self.mock_cursor_obj.insert_all.side_effect = exceptions.AirflowException()
    expected_event = (self.expected_event_row['position'],
                      json.loads(self.expected_event_row['info']),
                      self.expected_event_row['type_id'])
    self.expected_event_row['timestamp'] = (monitoring_hook.
                                            _generate_zone_aware_timestamp())

    with self.assertRaises(errors.MonitoringAppendLogError):
      self.hook.store_events(dag_name=self.expected_event_row['dag_name'],
                             location=self.expected_event_row['location'],
                             id_event_error_tuple_list=[expected_event])

  def test_store_retry(self):
    self.hook.store_retry(dag_name=self.expected_retry_row['dag_name'],
                          timestamp=self.expected_retry_row['timestamp'],
                          location=self.expected_retry_row['location'])

    self.mock_cursor_obj.insert_all.assert_called_once_with(
        project_id=self.project_id, dataset_id=self.dataset_id,
        table_id=self.table_id, rows=[{'json': self.expected_retry_row}])

  def test_store_retry_creates_timestamp_when_none_provided(self):
    self.expected_retry_row['timestamp'] = (monitoring_hook.
                                            _generate_zone_aware_timestamp())

    self.hook.store_retry(dag_name=self.expected_retry_row['dag_name'],
                          location=self.expected_retry_row['location'])

    self.mock_cursor_obj.insert_all.assert_called_once_with(
        project_id=self.project_id, dataset_id=self.dataset_id,
        table_id=self.table_id, rows=[{'json': self.expected_retry_row}])

  def test_store_retry_handles_storing_error(self):
    self.mock_cursor_obj.insert_all.side_effect = exceptions.AirflowException()
    self.expected_retry_row['timestamp'] = (monitoring_hook.
                                            _generate_zone_aware_timestamp())

    with self.assertRaises(errors.MonitoringAppendLogError):
      self.hook.store_retry(dag_name=self.expected_retry_row['dag_name'],
                            location=self.expected_retry_row['location'])

  def test_generate_processed_blobs_position_ranges(self):
    self.mock_cursor_obj.execute = mock.MagicMock()
    self.mock_cursor_obj.fetchone.side_effect = [('0', '1000'),
                                                 ('1000', '1'), None]
    gen = self.hook.generate_processed_blobs_ranges()

    self.assertTupleEqual(('0', '1000'), next(gen))
    self.assertTupleEqual(('1000', '1'), next(gen))
    with self.assertRaises(StopIteration):
      next(gen)
    self.mock_cursor_obj.execute.assert_called_once()

  def test_events_blobs_generator(self):
    self.mock_cursor_obj.execute = mock.MagicMock()
    self.mock_cursor_obj.fetchone.side_effect = [['{"a": "1"}'], ['{"b": 2}'],
                                                 None]
    gen = self.hook.events_blobs_generator()

    blb = next(gen)
    self.assertListEqual([{'a': '1'}, {'b': 2}], blb.events)
    self.mock_cursor_obj.execute.assert_called_once()

  def test_events_blobs_generator_2_blobs(self):
    self.mock_cursor_obj.execute = mock.MagicMock()
    self.mock_cursor_obj.fetchone.side_effect = (
        [['{"a": "1"}']] * (monitoring_hook._DEFAULT_PAGE_SIZE + 1) +
        [['{"b": 2}'], None])
    gen = self.hook.events_blobs_generator()

    blb = next(gen)
    self.assertListEqual(
        [{'a': '1'}]*(monitoring_hook._DEFAULT_PAGE_SIZE), blb.events)
    blb = next(gen)
    self.assertListEqual([{'a': '1'}, {'b': 2}], blb.events)
    self.mock_cursor_obj.execute.assert_called_once()

  def test_events_blobs_generator_exactly_page_size(self):
    self.mock_cursor_obj.execute = mock.MagicMock()
    self.mock_cursor_obj.fetchone.side_effect = (
        [['{"a": "1"}']]*(monitoring_hook._DEFAULT_PAGE_SIZE) + [None])
    gen = self.hook.events_blobs_generator()

    blb = next(gen)
    self.assertListEqual(
        [{'a': '1'}]*(monitoring_hook._DEFAULT_PAGE_SIZE), blb.events)
    self.mock_cursor_obj.execute.assert_called_once()

  def test_events_blobs_generator_retry(self):
    self.mock_cursor_obj.execute = mock.MagicMock()
    self.mock_cursor_obj.fetchone.side_effect = [['{"a": "1"}'], ['{"b": 2}'],
                                                 None]

    with mock.patch.object(monitoring_hook.MonitoringHook, 'store_retry',
                           autospec=True):
      gen = self.hook.events_blobs_generator()
      next(gen)

      self.hook.store_retry.assert_called_once()

  def test_events_blobs_generator_no_retry(self):
    self.mock_cursor_obj.execute = mock.MagicMock()
    self.mock_cursor_obj.fetchone.side_effect = [['{"a": "1"}'], ['{"b": 2}'],
                                                 None]

    with mock.patch.object(monitoring_hook.MonitoringHook, 'store_retry',
                           autospec=True):
      self.hook.enable_monitoring = False
      gen = self.hook.events_blobs_generator()
      next(gen)

      self.hook.store_retry.assert_not_called()

  def test_cleanup_by_days_to_live(self):
    time_to_live = 1
    cutoff_timestamp = (datetime.datetime.utcnow() - datetime.timedelta(
        days=time_to_live)).isoformat() + 'Z'
    cleanup_sql = (f'DELETE FROM `{self.dataset_id}.{self.table_id}` WHERE '
                   f'`timestamp`<%(cutoff_timestamp)s')
    params = {'cutoff_timestamp': cutoff_timestamp}

    self.mock_cursor_obj.execute = mock.MagicMock()

    self.hook.cleanup_by_days_to_live(days_to_live=time_to_live)

    self.mock_cursor_obj.execute.assert_called_once_with(cleanup_sql, params)

  def test_cleanup_by_days_to_live_with_no_ttl_raises_error(self):
    with self.assertRaises(errors.MonitoringCleanupError):
      self.hook.cleanup_by_days_to_live(days_to_live=None)

  def test_cleanup_by_days_to_live_with_ttl_less_than_one_raises_error(self):
    with self.assertRaises(errors.MonitoringCleanupError):
      self.hook.cleanup_by_days_to_live(days_to_live=-1)


if __name__ == '__main__':
  unittest.main()
