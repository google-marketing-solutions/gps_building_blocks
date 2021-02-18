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

"""Tests for gps_building_blocks.tcrm.hooks.gcs_hook."""

import json
import unittest

from unittest import mock
from airflow.contrib.hooks import gcp_api_base_hook
from airflow.contrib.hooks import gcs_hook as base_gcs_hook
from google.api_core.exceptions import NotFound

from gps_building_blocks.tcrm.hooks import gcs_hook
from gps_building_blocks.tcrm.utils import errors


def fake_generator(expected):
  """Fake for _gcs_blob_chunk_generator."""
  for chunk in expected:
    if isinstance(chunk, bytes):
      yield chunk
    else:
      yield chunk.encode()  # encode as bytes


def get_expected(expected, content_type=gcs_hook.BlobContentTypes.JSON.name):
  """Transforms a list into the output expected from _load_blob_into_queue.

  Input must be a list of bytes.

  Args:
    expected: List containing expected content from GCS Blobs.
    content_type: Blob's content type described by BlobContentTypes.

  Returns:
    Object containing parsed representation of the GCS Blob.
  """
  events = b''.join(expected).splitlines()
  if content_type == gcs_hook.BlobContentTypes.JSON.name:
    return json.loads('[{}]'.format(b','.join(events).decode('utf-8')))
  else:
    fields = events[0].decode('utf-8').split(',')
    return [dict(zip(fields, event.decode('utf-8').split(',')))
            for event in events[1:]]


class GoogleCloudStorageHookTest(unittest.TestCase):

  def setUp(self):
    super(GoogleCloudStorageHookTest, self).setUp()
    self.addCleanup(mock.patch.stopall)

    self.mock_bucket_name = 'bucket'
    self.mock_prefix = 'prefix'

    with mock.patch.object(gcp_api_base_hook.GoogleCloudBaseHook, '__init__',
                           autospec=True):
      self.gcs_hook = gcs_hook.GoogleCloudStorageHook(
          gcs_bucket=self.mock_bucket_name,
          gcs_content_type='JSON',
          gcs_prefix=self.mock_prefix)
    patcher_get_conn = mock.patch.object(
        base_gcs_hook.GoogleCloudStorageHook, 'get_conn', autospec=True).start()

    mock_conn = mock.MagicMock()
    patcher_get_conn.return_value = mock_conn

    self.mock_bucket = mock.MagicMock()
    mock_conn.bucket.return_value = self.mock_bucket

    chunk_size = 100 * 1024 * 1024

    self.mock_file_blob = mock.MagicMock()
    self.mock_file_blob.size = chunk_size + 1
    self.mock_bucket.get_blob.return_value = self.mock_file_blob

    mock.patch.object(
        gcs_hook.GoogleCloudStorageHook, '_parse_events_by_content_type',
        autospec=True).start()

  def test_raise_error_when_content_type_is_incorrect(self):
    with self.assertRaises(errors.DataInConnectorValueError):
      gcs_hook.GoogleCloudStorageHook(gcs_bucket='bucket',
                                      gcs_content_type='BAD',
                                      gcs_prefix='')

  def test_download_chunk_by_chunk(self):
    self.gcs_hook.get_blob_events('blob_name')
    self.mock_file_blob.download_to_file.assert_called()

  def test_raise_error_when_failed_to_get_blob(self):
    self.mock_bucket.get_blob.side_effect = NotFound(message='not found')
    with self.assertRaises(errors.DataInConnectorError):
      self.gcs_hook.get_blob_events('blob_name')

  def test_raise_error_when_blob_is_none(self):
    self.mock_bucket.get_blob.return_value = None
    with self.assertRaises(errors.DataInConnectorError):
      self.gcs_hook.get_blob_events('blob_name')

  def test_raise_error_when_failed_to_download_blob(self):
    self.mock_file_blob.download_to_file.side_effect = NotFound(
        message='not found')
    with self.assertRaises(errors.DataInConnectorError):
      self.gcs_hook.get_blob_events('blob_name')

  def test_get_location(self):
    loc = self.gcs_hook.get_location()
    self.assertEqual(loc, f'gs://{self.mock_bucket_name}/{self.mock_prefix}')


class JSONGoogleCloudStorageHookTest(unittest.TestCase):

  def setUp(self):
    super(JSONGoogleCloudStorageHookTest, self).setUp()
    self.addCleanup(mock.patch.stopall)

    with mock.patch.object(gcp_api_base_hook.GoogleCloudBaseHook, '__init__',
                           autospec=True):
      self.gcs_hook = gcs_hook.GoogleCloudStorageHook(gcs_bucket='bucket',
                                                      gcs_content_type='JSON',
                                                      gcs_prefix='')

    self.mocked_conn = mock.patch.object(base_gcs_hook.GoogleCloudStorageHook,
                                         'get_conn',
                                         autospec=True).start()
    self.mocked_conn.return_value.objects = mock.MagicMock()

    self.mocked_list = mock.patch.object(base_gcs_hook.GoogleCloudStorageHook,
                                         'list', autospec=True).start()

    self.patched_chunk_generator = mock.patch.object(
        gcs_hook.GoogleCloudStorageHook, '_gcs_blob_chunk_generator',
        autospec=True).start()

  def test_blob_loaded_successfully(self):
    expected = [b'{"a": 1}\n{"b": 2}']
    self.patched_chunk_generator.return_value = fake_generator(expected)

    events = self.gcs_hook.get_blob_events(blob_name='blob')

    self.assertListEqual(events, get_expected(expected))

  def test_handles_empty_file(self):
    self.patched_chunk_generator.return_value = fake_generator([])

    events = self.gcs_hook.get_blob_events(blob_name='blob')

    self.assertListEqual(events, [])

  def test_handles_blank_line(self):
    self.patched_chunk_generator.return_value = fake_generator([''])

    events = self.gcs_hook.get_blob_events(blob_name='blob')

    self.assertListEqual(events, [])

  def test_handles_line_break_at_beginning_of_chunck(self):
    line_break_at_beginning_list = [b'{"c": 3}', b'\n{"E": 5}']
    self.patched_chunk_generator.return_value = fake_generator(
        line_break_at_beginning_list)

    events = self.gcs_hook.get_blob_events(blob_name='blob')

    self.assertListEqual(events, get_expected(line_break_at_beginning_list))

  def test_handles_broken_json(self):
    broken_json_list = [b'{"c": 3}\n{"D":', b' 4}', b'\n{"E": 5}']
    self.patched_chunk_generator.return_value = fake_generator(broken_json_list)

    events = self.gcs_hook.get_blob_events(blob_name='blob')

    self.assertListEqual(events, get_expected(broken_json_list))

  def test_raises_error_when_broken_json_at_the_end_of_blob(self):
    self.patched_chunk_generator.return_value = fake_generator([b'"E": 5}'])

    with self.assertRaises(errors.DataInConnectorBlobParseError):
      self.gcs_hook.get_blob_events(blob_name='blob')

  def test_raises_error_when_parsing_bad_json(self):
    self.patched_chunk_generator.return_value = fake_generator(
        [b'{"c": 3\n{"D": 4}'])

    with self.assertRaises(errors.DataInConnectorBlobParseError):
      self.gcs_hook.get_blob_events(blob_name='blob')

  def test_handles_broken_multibyte_character(self):
    broken_multybyte_list = [b'{"c": "', b'\xf0\x92', b'\x88\x99', b'"}']
    self.patched_chunk_generator.return_value = fake_generator(
        broken_multybyte_list)

    events = self.gcs_hook.get_blob_events(blob_name='blob')

    self.assertListEqual(events, get_expected(broken_multybyte_list))

  def test_large_list(self):
    # 1,000,000 instances of {"key": "val"}
    long_list = [b'\n'.join([b'{"key": "val"}'] * 1000) + b'\n'] * 1000
    self.patched_chunk_generator.return_value = fake_generator(long_list)

    events = self.gcs_hook.get_blob_events(blob_name='blob')

    self.assertListEqual(events, get_expected(long_list))

  def test_events_blobs_generator(self):
    self.mocked_list.return_value = ['blob_1', 'blob_2', 'blob_3']
    expected = [{'a': 1}]
    with mock.patch.object(gcs_hook.GoogleCloudStorageHook, 'get_blob_events',
                           autospec=True, return_value=expected):
      blobs_generator = self.gcs_hook.events_blobs_generator()

      self.assertListEqual(
          [(blb.events, blb.location, blb.position) for blb in blobs_generator],
          [(expected, self.gcs_hook.get_location(), 'blob_{}'.format(i)
           ) for i in range(1, 4)])

  def test_events_blobs_generator_with_erroneouse_blobs(self):
    self.mocked_list.return_value = ['blob_1']
    error = errors.DataInConnectorBlobParseError(msg='bad_blob')
    with mock.patch.object(gcs_hook.GoogleCloudStorageHook, 'get_blob_events',
                           autospec=True, side_effect=error):
      blobs_generator = self.gcs_hook.events_blobs_generator()

      self.assertListEqual(list(blobs_generator), [])

  def test_events_blobs_generator_raises_data_in_connector_error(self):
    self.mocked_list.side_effect = errors.DataInConnectorError()

    with self.assertRaises(errors.DataInConnectorError):
      self.gcs_hook.events_blobs_generator().__next__()


class CSVGoogleCloudStorageHookTest(unittest.TestCase):

  def setUp(self):
    super(CSVGoogleCloudStorageHookTest, self).setUp()
    self.addCleanup(mock.patch.stopall)

    with mock.patch.object(gcp_api_base_hook.GoogleCloudBaseHook, '__init__',
                           autospec=True):
      self.gcs_hook = gcs_hook.GoogleCloudStorageHook(
          gcs_bucket='bucket',
          gcs_content_type=gcs_hook.BlobContentTypes.CSV.name,
          gcs_prefix='')

    self.mocked_conn = mock.patch.object(base_gcs_hook.GoogleCloudStorageHook,
                                         'get_conn',
                                         autospec=True).start()
    self.mocked_conn.return_value.objects = mock.MagicMock()

    self.mocked_list = mock.patch.object(base_gcs_hook.GoogleCloudStorageHook,
                                         'list', autospec=True).start()

    self.patched_chunk_generator = mock.patch.object(
        gcs_hook.GoogleCloudStorageHook, '_gcs_blob_chunk_generator',
        autospec=True).start()

  def test_blob_loaded_successfully(self):
    expected = [b'field1,field2,field3\n1,2,3\n']
    self.patched_chunk_generator.return_value = fake_generator(expected)

    events = self.gcs_hook.get_blob_events(blob_name='blob')

    self.assertListEqual(events, get_expected(expected,
                                              self.gcs_hook.content_type))

  def test_handles_empty_file(self):
    self.patched_chunk_generator.return_value = fake_generator([])

    events = self.gcs_hook.get_blob_events(blob_name='blob')

    self.assertListEqual(events, [])

  def test_handles_only_fields_line(self):
    only_one_fields_line = [b'field1,field2,field3\n']
    self.patched_chunk_generator.return_value = fake_generator(
        only_one_fields_line)

    events = self.gcs_hook.get_blob_events(blob_name='blob')

    self.assertListEqual(events, get_expected(only_one_fields_line,
                                              self.gcs_hook.content_type))

  def test_handles_blank_line(self):
    self.patched_chunk_generator.return_value = fake_generator([''])

    events = self.gcs_hook.get_blob_events(blob_name='blob')

    self.assertListEqual(events, [])

  def test_handles_line_break_at_beginning_of_chunck(self):
    line_break_at_beginning_list = [b'field1,field2,field3', b'\n1,2,3\n']
    self.patched_chunk_generator.return_value = fake_generator(
        line_break_at_beginning_list)

    events = self.gcs_hook.get_blob_events(blob_name='blob')

    self.assertListEqual(events, get_expected(line_break_at_beginning_list,
                                              self.gcs_hook.content_type))

  def test_handles_broken_csv_lines(self):
    broken_json_list = [b'field1,field2,field3\n',
                        b'1,', b'2,3\n', b'1,2', b',3\n']
    self.patched_chunk_generator.return_value = fake_generator(broken_json_list)

    events = self.gcs_hook.get_blob_events(blob_name='blob')

    self.assertListEqual(events, get_expected(broken_json_list,
                                              self.gcs_hook.content_type))

  def test_raises_error_when_csv_field_count_is_different(self):
    self.patched_chunk_generator.return_value = fake_generator(
        [b'field1,field2,field3\n', b'1,2,3\n', b'1,2'])

    with self.assertRaises(errors.DataInConnectorBlobParseError):
      self.gcs_hook.get_blob_events(blob_name='blob')

  def test_handles_broken_multibyte_character(self):
    broken_multybyte_list = [b'field1,field2,field3\n1,\xf0\x92',
                             b'\x88\x99,3\n']
    self.patched_chunk_generator.return_value = fake_generator(
        broken_multybyte_list)

    events = self.gcs_hook.get_blob_events(blob_name='blob')

    self.assertListEqual(events, get_expected(broken_multybyte_list,
                                              self.gcs_hook.content_type))

  def test_large_list(self):
    # 1,000,000 instances of {"key": "val"}
    long_list = [b'field1,field2,field3\n' + b'\n'.join(
        [b'1,2,3'] * 1000) + b'\n'] * 1000
    self.patched_chunk_generator.return_value = fake_generator(long_list)

    events = self.gcs_hook.get_blob_events(blob_name='blob')

    self.assertListEqual(events, get_expected(long_list,
                                              self.gcs_hook.content_type))

  def test_events_blobs_generator(self):
    self.mocked_list.return_value = ['blob_1', 'blob_2', 'blob_3']
    expected = [{'a': 1}]
    with mock.patch.object(gcs_hook.GoogleCloudStorageHook, 'get_blob_events',
                           autospec=True, return_value=expected):
      blobs_generator = self.gcs_hook.events_blobs_generator()

      self.assertListEqual(
          [(blb.events, blb.location, blb.position) for blb in blobs_generator],
          [(expected, self.gcs_hook.get_location(), 'blob_{}'.format(i)
           ) for i in range(1, 4)])

  def test_events_blobs_generator_with_erroneous_blobs(self):
    self.mocked_list.return_value = ['blob_1']
    error = errors.DataInConnectorBlobParseError(msg='bad_blob')
    with mock.patch.object(gcs_hook.GoogleCloudStorageHook, 'get_blob_events',
                           autospec=True, side_effect=error):
      blobs_generator = self.gcs_hook.events_blobs_generator()

      self.assertListEqual(list(blobs_generator), [])

  def test_events_blobs_generator_raises_data_in_connector_error(self):
    self.mocked_list.side_effect = errors.DataInConnectorError()

    with self.assertRaises(errors.DataInConnectorError):
      self.gcs_hook.events_blobs_generator().__next__()

  def test_events_blobs_generator_read_once(self):
    self.mocked_list.return_value = ['blob_1', 'blob_2', 'blob_3', 'blob_4']
    events = [{'a': 1}]
    expected = [
        ([{
            'a': 1
        }], 'gs://bucket/', 'blob_1'),
        ([{
            'a': 1
        }], 'gs://bucket/', 'blob_4'),
    ]
    with mock.patch.object(gcs_hook.GoogleCloudStorageHook, 'get_blob_events',
                           autospec=True, return_value=events):
      processed_files = [('blob_2', '1000'), ('blob_3', '1000')]
      processed_generator = (
          processed_files[i] for i in range(0, len(processed_files)))
      blobs_generator = self.gcs_hook.events_blobs_generator(
          processed_blobs_generator=processed_generator
      )

      actual = [(blb.events, blb.location, blb.position)
                for blb in blobs_generator]
      self.assertListEqual(expected, actual)


if __name__ == '__main__':
  unittest.main()

