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

# Lint as: python3
"""Tests for google3.third_party.gps_building_blocks.cloud.utils.cloud_storage."""

import os
import unittest

import mock

from google.api_core import exceptions
from google.auth import credentials
from google.cloud import storage
from google3.testing.pybase import parameterized
from gps_building_blocks.cloud.utils import cloud_auth
from gps_building_blocks.cloud.utils import cloud_storage


class CloudStorageTest(parameterized.TestCase):

  def setUp(self):
    super(CloudStorageTest, self).setUp()
    self.addCleanup(mock.patch.stopall)
    # Mock for google.cloud.storage.Client object
    self.project_id = 'project-id'
    self.mock_client = mock.patch.object(
        storage, 'Client', autospec=True).start()
    self.mock_get_credentials = mock.patch.object(
        cloud_auth, 'get_credentials', autospec=True).start()
    self.mock_credentials = mock.Mock(credentials.Credentials, autospec=True)
    self.mock_get_credentials.return_value = self.mock_credentials
    self.source_file_path = '/tmp/file.txt'
    self.source_directory_path = '/tmp/dir1'
    self.destination_blob_path = 'dir1/dir2/blob'
    self.bucket_name = 'bucket_name'
    self.destination_blob_url = (f'gs://{self.bucket_name}/'
                                 f'{self.destination_blob_path}')
    self.mock_is_file = mock.patch.object(
        os.path, 'isfile', autospec=True).start()
    self.mock_is_dir = mock.patch.object(
        os.path, 'isdir', autospec=True).start()
    self.mock_bucket = mock.Mock(storage.Bucket, autospec=True)
    self.mock_blob = mock.Mock(storage.Blob, autospec=True)
    self.mock_client.return_value.get_bucket.return_value = self.mock_bucket
    self.mock_bucket.blob.return_value = self.mock_blob
    self.file_content = 'Content of the file.'
    self.cloud_storage_obj = cloud_storage.CloudStorageUtils(self.project_id)

  def test_parse_blob_url(self):
    bucket_name, path = self.cloud_storage_obj._parse_blob_url(
        self.destination_blob_url)

    self.assertEqual(bucket_name, self.bucket_name)
    self.assertEqual(path, self.destination_blob_path)

  def test_parse_blob_url_raises_error_for_invalid_url(self):
    with self.assertRaises(cloud_storage.Error):
      self.cloud_storage_obj._parse_blob_url('invalid_url')

  def test_get_or_create_bucket_method_creates_bucket_if_it_does_not_exist(
      self):
    self.mock_client.return_value.get_bucket.side_effect = exceptions.NotFound(
        'Bucket Not Found')
    self.mock_client.return_value.create_bucket.return_value = self.mock_bucket

    self.cloud_storage_obj._get_or_create_bucket(self.bucket_name)

    self.mock_client.return_value.create_bucket.assert_called_once_with(
        self.bucket_name)

  def test_get_or_create_bucket_method_does_not_create_bucket_if_it_already_exists(
      self):
    self.cloud_storage_obj._get_or_create_bucket(self.bucket_name)

    self.mock_client.return_value.create_bucket.assert_not_called()

  def test_get_or_create_bucket_method_is_retried_on_transient_errors(self):
    # get_bucket raises TooManyRequests error when it's called for the first
    # time but runs successfully the next time.
    self.mock_client.return_value.get_bucket.side_effect = [
        exceptions.TooManyRequests('Too many requests.'), self.mock_bucket
    ]

    self.cloud_storage_obj._get_or_create_bucket(self.bucket_name)

    self.assertEqual(2, self.mock_client.return_value.get_bucket.call_count)

  @parameterized.named_parameters(
      ('service_account_file', '/tmp/service_account_key.json'),
      ('default', None),
  )
  def test_credential_retrieval_logic_when_initializing_cloud_storage_utils(
      self, service_account_key_file):
    cloud_storage_obj = cloud_storage.CloudStorageUtils(
        self.project_id, service_account_key_file)

    self.mock_get_credentials.assert_called_with(service_account_key_file)
    self.mock_client.assert_called_with(
        project=self.project_id, credentials=self.mock_credentials)
    self.assertEqual(cloud_storage_obj.client, self.mock_client.return_value)

  @mock.patch.object(
      cloud_storage.CloudStorageUtils, 'upload_file', autospec=True)
  def test_upload_file_to_url(self, mock_upload_file):
    self.cloud_storage_obj.upload_file_to_url(self.source_file_path,
                                              self.destination_blob_url)

    mock_upload_file.assert_called_once_with(self.cloud_storage_obj,
                                             self.source_file_path,
                                             self.bucket_name,
                                             self.destination_blob_path)

  def test_upload_file_to_cloud_storage(self):
    self.cloud_storage_obj.upload_file(self.source_file_path, self.bucket_name,
                                       self.destination_blob_path)

    self.mock_is_file.assert_called_once_with(self.source_file_path)
    self.mock_client.return_value.get_bucket.assert_called_once_with(
        self.bucket_name)
    self.mock_bucket.blob.assert_called_once_with(self.destination_blob_path)
    self.mock_blob.upload_from_filename.assert_called_once_with(
        self.source_file_path)

  def test_exception_is_raised_when_file_is_not_found(self):
    self.mock_is_file.return_value = False

    with self.assertRaises(FileNotFoundError):
      self.cloud_storage_obj.upload_file(self.source_file_path,
                                         self.bucket_name,
                                         self.destination_blob_path)

  def test_upload_file_is_retried_on_transient_errors(self):
    # Upload file raises TooManyRequests error when it's called for the first
    # time but runs successfully the next time.
    self.mock_blob.upload_from_filename.side_effect = [
        exceptions.TooManyRequests('Too many requests.'), None
    ]

    self.cloud_storage_obj._upload_file(self.source_file_path, self.mock_bucket,
                                        self.destination_blob_path)

    self.assertEqual(2, self.mock_blob.upload_from_filename.call_count)

  @mock.patch.object(
      cloud_storage.CloudStorageUtils, '_upload_file', autospec=True)
  def test_upload_file_raises_error_on_retry_exception(self, mock_upload_file):
    mock_upload_file.side_effect = exceptions.RetryError('Message', 'Cause')

    with self.assertRaises(cloud_storage.Error):
      self.cloud_storage_obj.upload_file(self.source_file_path,
                                         self.bucket_name,
                                         self.destination_blob_path)

  @mock.patch.object(
      cloud_storage.CloudStorageUtils, 'upload_directory', autospec=True)
  def test_upload_directory_to_url(self, mock_upload_directory):
    self.cloud_storage_obj.upload_directory_to_url(self.source_directory_path,
                                                   self.destination_blob_url)

    mock_upload_directory.assert_called_once_with(self.cloud_storage_obj,
                                                  self.source_directory_path,
                                                  self.bucket_name,
                                                  self.destination_blob_path)

  @mock.patch.object(
      cloud_storage.CloudStorageUtils, '_upload_file', autospec=True)
  @mock.patch.object(os, 'walk', autospec=True)
  def test_upload_directory(self, mock_walk, mock_upload_file):
    destination_blob_path = 'dir1'
    file_structure = [['/tmp/dir1', ['dir2', 'dir3'], ['file1', 'file2']],
                      ['/tmp/dir1/dir2', [], ['file3']],
                      ['/tmp/dir1/dir3', ['dir4'], ['file4']],
                      ['/tmp/dir1/dir3/dir4', [], []]]
    mock_walk.return_value = file_structure
    calls = []
    for (root, _, files) in file_structure:
      for file in files:
        source_file = root + '/' + file
        call = mock.call(
            self.cloud_storage_obj, root + '/' + file, self.mock_bucket,
            source_file.replace(self.source_directory_path,
                                destination_blob_path))
        calls.append(call)

    self.cloud_storage_obj.upload_directory(self.source_directory_path,
                                            self.mock_bucket,
                                            destination_blob_path)

    mock_upload_file.assert_has_calls(calls)

  def test_exception_is_raised_when_directory_is_not_found(self):
    self.mock_is_dir.return_value = False
    invalid_dir = '/tmp/invalid_dir'

    with self.assertRaises(FileNotFoundError):
      self.cloud_storage_obj.upload_directory(invalid_dir, self.bucket_name,
                                              self.destination_blob_path)

  def test_write_to_path(self):
    path = 'gs://bucket/dir/file'
    self.cloud_storage_obj.write_to_path(self.file_content, path)

    self.mock_blob.upload_from_string.assert_called_once_with(self.file_content)

  def test_write_to_file(self):
    self.cloud_storage_obj.write_to_file(self.file_content, self.bucket_name,
                                         self.destination_blob_path)

    self.mock_blob.upload_from_string.assert_called_once_with(self.file_content)

  def test_write_to_file_is_retried_on_transient_errors(self):
    # upload_from_string raises TooManyRequests error when it's called for the
    # first time but runs successfully the next time.
    self.mock_blob.upload_from_string.side_effect = [
        exceptions.TooManyRequests('Too many requests.'), None
    ]

    self.cloud_storage_obj.write_to_file(self.file_content, self.bucket_name,
                                         self.destination_blob_path)

    self.assertEqual(2, self.mock_blob.upload_from_string.call_count)


if __name__ == '__main__':
  unittest.main()
