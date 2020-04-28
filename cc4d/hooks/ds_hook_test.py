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

"""Unit tests for cc4d.hooks.cc4d.ds_hook."""

import unittest
import unittest.mock as mock
import freezegun

from googleapiclient import errors as googleapiclient_errors

from gps_building_blocks.cc4d.hooks import ds_hook
from gps_building_blocks.cc4d.utils import blob
from gps_building_blocks.cc4d.utils import errors
from gps_building_blocks.cc4d.utils import retry_utils

_FAKE_RETRIABLE_HTTP_ERROR = googleapiclient_errors.HttpError(
    mock.Mock(status=429),
    bytes('[{"error": {"message": "error"}}]', 'utf-8'))


class DatastoreHookTest(unittest.TestCase):

  def setUp(self):
    self.addCleanup(mock.patch.stopall)
    super(DatastoreHookTest, self).setUp()
    self.base_hook_init_patcher = mock.patch(
        'airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.__init__',
        autospec=True)
    self.mock_base_hook_init = self.base_hook_init_patcher.start()
    self.get_conn_patcher = mock.patch(
        'airflow.contrib.hooks.datastore_hook.DatastoreHook.get_conn')
    self.mock_get_conn = self.get_conn_patcher.start()
    self.datastore_hook = ds_hook.DatastoreHook()

    self.datastore_hook.commit = mock.MagicMock()
    self.datastore_hook.run_query = mock.MagicMock()

    self.blob = blob.Blob(events=[], blob_id='id', platform='GCS',
                          source='bucket', location='blob')

  def test_commit_with_retries_on_retriable_http_error(self):
    self.datastore_hook.commit = mock.MagicMock()
    self.datastore_hook.commit.side_effect = _FAKE_RETRIABLE_HTTP_ERROR

    try:
      self.datastore_hook.commit_with_retries_on_retriable_http_error(body={})
    except googleapiclient_errors.HttpError:
      pass

    self.assertEqual(self.datastore_hook.commit.call_count,
                     retry_utils._RETRY_UTILS_MAX_RETRIES)

  @freezegun.freeze_time('2019-01-01 00:00:00.000000')
  def test_insert_blob_information_should_call_commit(self):
    expected_body = self.datastore_hook._build_body_for_commit(
        self.blob, ds_hook._COMMIT_METHOD_INSERT)

    self.datastore_hook.insert_blob_information(self.blob)

    self.datastore_hook.commit.assert_called_once_with(body=expected_body)

  def test_insert_blob_information_when_datastore_returns_error(self):
    self.datastore_hook.commit = mock.MagicMock(
        side_effect=_FAKE_RETRIABLE_HTTP_ERROR)

    with self.assertRaises(errors.DatastoreInsertBlobInfoError):
      self.datastore_hook.insert_blob_information(self.blob)

  @freezegun.freeze_time('2019-01-01 00:00:00.000000')
  def test_update_blob_information_should_call_commit(self):
    expected_body = self.datastore_hook._build_body_for_commit(
        self.blob, ds_hook._COMMIT_METHOD_UPDATE)

    self.datastore_hook.update_blob_information(self.blob)

    self.datastore_hook.commit.assert_called_once_with(body=expected_body)

  @freezegun.freeze_time('2019-01-01 00:00:00.000000')
  def test_insert_or_update_blob_information_call_commit_insert_once(self):
    expected_body = self.datastore_hook._build_body_for_commit(
        self.blob, ds_hook._COMMIT_METHOD_INSERT)

    self.datastore_hook.insert_or_update_blob_information(self.blob)

    self.datastore_hook.commit.assert_called_once_with(body=expected_body)

  @freezegun.freeze_time('2019-01-01 00:00:00.000000')
  def test_insert_or_update_blob_information_call_commit_update_on_error(self):
    expected_body = self.datastore_hook._build_body_for_commit(
        self.blob, ds_hook._COMMIT_METHOD_UPDATE)
    self.datastore_hook.insert_blob_information = mock.MagicMock(
        side_effect=errors.DatastoreInsertBlobInfoError())

    self.datastore_hook.insert_or_update_blob_information(self.blob)

    self.datastore_hook.commit.assert_called_once_with(body=expected_body)

  def test_update_blob_information_when_datastore_returns_error(self):
    self.datastore_hook.commit = mock.MagicMock(
        side_effect=_FAKE_RETRIABLE_HTTP_ERROR)

    with self.assertRaises(errors.DatastoreUpdateBlobInfoError):
      self.datastore_hook.update_blob_information(self.blob)

  def test_run_query_with_retries_on_retriable_http_error(self):
    self.datastore_hook.run_query = mock.MagicMock(
        side_effect=_FAKE_RETRIABLE_HTTP_ERROR)

    try:
      self.datastore_hook.run_query_with_retries_on_retriable_http_error(
          body={})
    except googleapiclient_errors.HttpError:
      pass

    self.assertEqual(self.datastore_hook.run_query.call_count,
                     retry_utils._RETRY_UTILS_MAX_RETRIES)

  def test_generate_blobs_with_status_when_datastore_returns_error(self):
    self.datastore_hook.run_query = mock.MagicMock(
        side_effect=_FAKE_RETRIABLE_HTTP_ERROR)

    with self.assertRaises(errors.DatastoreRunQueryError):
      blobs = self.datastore_hook.generate_blobs_with_status(
          source='bucket', status=blob.BlobStatus.ERROR)
      list(blb for blb in blobs)

  def test_get_blobs_with_status_when_entities_meet_condition(self):
    body = self.datastore_hook._build_body_for_commit(
        self.blob, ds_hook._COMMIT_METHOD_UPDATE)
    self.datastore_hook.run_query.return_value = {
        'entityResultType': 'FULL',
        'entityResults': [{
            'entity': {
                'key': {
                    'partitionId': {
                        'projectId': 'project'
                    },
                    'path': [{
                        'kind': ds_hook._ENTITY,
                        'name': self.blob.blob_id
                    }]
                },
                'properties': body['mutations'][0]['update']['properties'],
            },
            'cursor': 'dummy_cursor',
            'version': '12345'
        }],
        'endCursor': 'dummy_end_cursor',
        'moreResults': 'NO_MORE_RESULTS'
    }

    blobs = self.datastore_hook.generate_blobs_with_status(
        source='bucket', status=blob.BlobStatus.ERROR)

    for blb in blobs:
      self.assertTupleEqual((blb.events, blb.blob_id, blb.platform, blb.source,
                             blb.location, blb.position, blb.status,
                             blb.status_desc, blb.unsent_events_indexes),
                            (self.blob.events, self.blob.blob_id,
                             self.blob.platform, self.blob.source,
                             self.blob.location, self.blob.position,
                             self.blob.status, self.blob.status_desc,
                             self.blob.unsent_events_indexes))

  def test_generate_blobs_with_status_returns_empty_list_when_no_results_found(
      self):
    self.datastore_hook.run_query.return_value = {
        'entityResultType': 'FULL',
        'endCursor': 'dummy_end_cursor',
        'moreResults': 'NO_MORE_RESULTS'
    }
    expected_body = (self.datastore_hook.
                     _build_body_for_get_all_blobs_with_status_query(
                         source='bucket', status=blob.BlobStatus.ERROR))

    blobs = self.datastore_hook.generate_blobs_with_status(
        source='bucket', status=blob.BlobStatus.ERROR)
    blobs = list(blb for blb in blobs)

    self.datastore_hook.run_query.assert_called_with(body=expected_body)
    self.assertListEqual([], blobs)


if __name__ == '__main__':
  unittest.main()
