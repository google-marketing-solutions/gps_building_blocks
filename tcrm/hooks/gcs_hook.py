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

"""Custom GCS Hook for generating blobs from GCS."""

import enum
import io
import json

from typing import Generator, List, Dict, Any, Tuple
from airflow.contrib.hooks import gcs_hook
from google.api_core.exceptions import NotFound
from googleapiclient import errors as googleapiclient_errors

from gps_building_blocks.tcrm.hooks import input_hook_interface
from gps_building_blocks.tcrm.utils import blob
from gps_building_blocks.tcrm.utils import errors

_PLATFORM = 'GCS'
_START_POSITION_IN_BLOB = 0

# The default size in bytes (100MB) of each download chunk.
# The value is from googleapiclient http package.
_DEFAULT_CHUNK_SIZE = 100 * 1024 * 1024


class BlobContentTypes(enum.Enum):
  JSON = enum.auto()
  CSV = enum.auto()


class GoogleCloudStorageHook(gcs_hook.GoogleCloudStorageHook,
                             input_hook_interface.InputHookInterface):
  """Extends the Google Cloud Storage hook.

  Used for chunked download of blobs, and blob generation.

  The Blobs must satisfy the following conditions:
    - Content is formatted as newline-delimited events.
    - Content is formatted as UTF-8.
    - Content is validly formatted as one of the types in BlobContentTypes.
    - The first line in a CSV blob is the fields labels

  Attributes:
      bucket: Unique name of the bucket holding the target blob.
      prefix: The path to a location within the bucket.
      content_type: Blob's content type described by BlobContentTypes.
  """

  def __init__(self, gcs_bucket: str,
               gcs_content_type: str,
               gcs_prefix: str,
               **kwargs) -> None:
    """Initiates GoogleCloudStorageHook.

    Args:
      gcs_bucket: Unique name of the bucket holding the target blob.
      gcs_content_type: Blob's content type described by BlobContentTypes.
      gcs_prefix: The path to a location within the bucket.
      **kwargs: Other optional arguments.
    """
    self._verify_content_type(gcs_content_type)

    self.bucket = gcs_bucket
    self.content_type = gcs_content_type
    self.prefix = gcs_prefix

    super().__init__()

  def get_location(self):
    """Retrieves the full url of the bucket from the GCS data source.

    Returns:
      The full url of the bucket
    """
    return f'gs://{self.bucket}/{self.prefix}'

  def _verify_content_type(self, content_type: str) -> None:
    """Validates content_type matches one of the supported formats.

    The content type must be one of the formats listed in BlobContentTypes.

    Args:
      content_type: GCS content type to verify.

    Raises:
      DataInConnectorValueError: If the content type format is invalid.
    """
    if content_type not in BlobContentTypes.__members__:
      raise errors.DataInConnectorValueError(
          'Invalid GCS blob content type. The supported types are: %s.' %
          ', '.join([name for name, item in BlobContentTypes.__members__.items(
              )]),
          errors.ErrorNameIDMap.GCS_HOOK_ERROR_INVALID_BLOB_CONTENT_TYPE)

  def _gcs_blob_chunk_generator(self, blob_name: str
                               ) -> Generator[bytes, None, None]:
    """Downloads and generates chunks from given blob.

    The base GoogleCloudStorageHook only allows downloading an entire file.
    To enable handling large files this class provides a chunk-wise download of
    bytes within the blob.

    Args:
      blob_name: Unique location within the bucket for the target blob.

    Yields:
      Chunks of the given blob, formatted as bytes.

    Raises:
      DataInConnectorError: When download failed.
    """
    outio = io.BytesIO()
    try:
      bucket = self.get_conn().bucket(self.bucket)
      file_blob = bucket.get_blob(blob_name)
    except NotFound as error:
      raise errors.DataInConnectorError(
          error=error, msg='Failed to download the blob.',
          error_num=errors.ErrorNameIDMap.GCS_HOOK_ERROR_MISSING_BLOB)

    if file_blob is None:
      raise errors.DataInConnectorError(
          msg='Failed to download the blob.',
          error_num=errors.ErrorNameIDMap.GCS_HOOK_ERROR_MISSING_BLOB)

    chunks = int(file_blob.size / _DEFAULT_CHUNK_SIZE) + 1
    for i in range(0, chunks):
      outio.truncate(0)
      outio.seek(0)

      start = i * (_DEFAULT_CHUNK_SIZE + 1)
      end = i * (_DEFAULT_CHUNK_SIZE + 1) + _DEFAULT_CHUNK_SIZE
      if end > file_blob.size:
        end = file_blob.size

      try:
        file_blob.download_to_file(outio, start=start, end=end)
      except NotFound as error:
        raise errors.DataInConnectorError(
            error=error, msg='Failed to download the blob.',
            error_num=errors.ErrorNameIDMap.GCS_HOOK_ERROR_MISSING_BLOB)

      self.log.debug('Blob loading: {}%'.format(int(i / chunks * 100)))
      yield outio.getvalue()

  def _parse_events_as_json(self, parsable_events: List[bytes]
                           ) -> List[Dict[Any, Any]]:
    """Parses a list of events as JSON.

    Args:
      parsable_events: Bytes events to parse.

    Returns:
      A list of events formatted as JSON.

    Raises:
      DataInConnectorBlobParseError: When parsing the blob was unsuccessful.
    """
    try:
      return [json.loads(event.decode('utf-8')) for event in parsable_events]
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
      raise errors.DataInConnectorBlobParseError(
          error=error, msg='Failed to parse the blob as JSON.',
          error_num=errors.ErrorNameIDMap.GCS_HOOK_ERROR_BAD_JSON_FORMAT_BLOB)

  def _parse_events_as_csv(self, parsable_events: List[bytes]
                          ) -> List[Dict[Any, Any]]:
    """Parses a list of events as CSV.

    Args:
      parsable_events: Bytes events to parse.

    Returns:
      A list of events formatted as CSV.

    Raises:
      DataInConnectorBlobParseError: When parsing the blob was unsuccessful.
    """
    try:
      fields = parsable_events[0].decode('utf-8').split(',')
      events = [dict(zip(fields, event.decode('utf-8').split(',')))
                for event in parsable_events[1:]]
    except (ValueError, UnicodeDecodeError) as error:
      raise errors.DataInConnectorBlobParseError(
          error=error, msg='Failed to parse the blob as CSV',
          error_num=errors.ErrorNameIDMap.GCS_HOOK_ERROR_BAD_CSV_FORMAT_BLOB)
    if not all(len(event) == len(fields) for event in events):
      raise errors.DataInConnectorBlobParseError(
          msg='Failed to parse CSV, not all lines have same length.',
          error_num=errors.ErrorNameIDMap
          .GCS_HOOK_ERROR_DIFFERENT_ROW_LENGTH_IN_CSV_BLOB)
    return events

  def _parse_events_by_content_type(self, parsable_events: List[bytes]
                                   ) -> List[Dict[Any, Any]]:
    """Parses a list of events as content_type.

    Args:
      parsable_events: Bytes events to parse.

    Returns:
      A list of events formatted as content_type.
    """
    if not parsable_events:
      return []
    if self.content_type == BlobContentTypes.CSV.name:
      return self._parse_events_as_csv(parsable_events)
    else:
      return self._parse_events_as_json(parsable_events)

  def get_blob_events(self, blob_name: str) -> List[Dict[Any, Any]]:
    """Gets blob's contents.

    Args:
      blob_name: The location and file name of the blob in the bucket.

    Returns:
      A list of events formatted as content_type.
    """
    events: List[bytes] = []
    buffer: bytes = b''

    blob_chunks_generator = self._gcs_blob_chunk_generator(blob_name=blob_name)
    for chunk in blob_chunks_generator:
      buffer += chunk
      if buffer.startswith(b'\n'):
        buffer = buffer[1:]

      events.extend(buffer.splitlines())
      # Last event might be incomplete. In this case we save the last line back
      # into the buffer
      buffer = events.pop() if not buffer.endswith(b'\n') and events else b''

    if buffer:
      events.append(buffer)

    return self._parse_events_by_content_type(events)

  def events_blobs_generator(
      self,
      processed_blobs_generator: Generator[Tuple[str, str], None, None] = None
      ) -> Generator[blob.Blob, None, None]:
    """Generates all blobs from the bucket's prefix location.

    Args:
      processed_blobs_generator: A generator that provides the processed blob
        information that helps skip read ranges.

    Yields:
      A generator that generates Blob objects from blob contents within a
      prefix location in the bucket.

    Raises:
      DataInConnectorError: When listing blob in bucket returns a HttpError.
    """
    try:
      blob_names = self.list(bucket=self.bucket, prefix=self.prefix)
    except googleapiclient_errors.HttpError as error:
      raise errors.DataInConnectorError(
          error=error, msg='Failed to get list of blobs from bucket.',
          error_num=errors.ErrorNameIDMap.RETRIABLE_GCS_HOOK_ERROR_HTTP_ERROR)

    if processed_blobs_generator is not None:
      for processed_file, _ in processed_blobs_generator:
        if processed_file in blob_names:
          blob_names.remove(processed_file)

    for blob_name in blob_names:
      if not blob_name.endswith('/'):
        try:
          events = self.get_blob_events(blob_name)
          yield blob.Blob(events=events, location=self.get_location(),
                          position=blob_name)
        except (errors.DataInConnectorBlobParseError,
                errors.DataInConnectorError) as error:
          continue
