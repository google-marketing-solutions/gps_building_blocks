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

from typing import Generator, Text, List, Dict, Any
from airflow.contrib.hooks import gcs_hook as base_hook
from googleapiclient import errors as googleapiclient_errors
from googleapiclient import http

from gps_building_blocks.airflow.hooks import input_hook_interface
from gps_building_blocks.airflow.utils import blob
from gps_building_blocks.airflow.utils import errors

_PLATFORM = 'GCS'
_START_POSITION_IN_BLOB = 0


class BlobContentTypes(enum.Enum):
  JSON = enum.auto()
  CSV = enum.auto()


class GoogleCloudStorageHook(base_hook.GoogleCloudStorageHook,
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

  def __init__(self, bucket: Text, content_type: Text, prefix: Text) -> None:
    """Initiates GoogleCloudStorageHook.

    Args:
      bucket: Unique name of the bucket holding the target blob.
      content_type: Blob's content type described by BlobContentTypes.
      prefix: The path to a location within the bucket.

    """
    self._verify_content_type(content_type)

    self.bucket = bucket
    self.content_type = content_type
    self.prefix = prefix

    super(GoogleCloudStorageHook, self).__init__()

  def _verify_content_type(self, content_type: Text) -> None:
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
              )]))

  def _gcs_blob_chunk_generator(self, blob_name: Text
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
    done = False
    outio = io.BytesIO()
    try:
      request = self.get_conn().objects().get_media(bucket=self.bucket,  # pytype: disable=attribute-error
                                                    object=blob_name)
      downloader = http.MediaIoBaseDownload(outio, request)
    except googleapiclient_errors.HttpError as error:
      raise errors.DataInConnectorError(
          error=error, msg='Failed to download the blob.')

    while not done:
      outio.truncate(0)
      outio.seek(0)

      try:
        status, done = downloader.next_chunk()
      except googleapiclient_errors.HttpError as error:
        raise errors.DataInConnectorError(
            error=error, msg='Failed to download the blob.')

      self.log.debug('Blob loading: {}%'.format(int(status.progress() * 100)))
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
          error=error, msg='Failed to parse the blob as JSON.')

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
          error=error, msg='Failed to parse the blob as CSV')
    if not all(len(event) == len(fields) for event in events):
      raise errors.DataInConnectorBlobParseError(
          msg='Failed to parse CSV, not all lines have same length.')
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

  def get_blob_events(self, blob_name: Text) -> List[Dict[Any, Any]]:
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

  def events_blobs_generator(self) -> Generator[blob.Blob, None, None]:
    """Generates all blobs from the bucket's prefix location.

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
          error=error, msg='Failed to get list of blobs from bucket.')

    for blob_name in blob_names:
      url = 'gs://{}/{}'.format(self.bucket, blob_name)
      # Exclude folders from uploading to Datastore.
      if not blob_name.endswith('/'):
        try:
          events = self.get_blob_events(blob_name)
          yield blob.Blob(events=events, blob_id=url, platform=_PLATFORM,
                          source=self.bucket, location=blob_name,
                          position=_START_POSITION_IN_BLOB)
        except (errors.DataInConnectorBlobParseError,
                errors.DataInConnectorError) as error:
          yield blob.Blob(events=[], blob_id=url, platform=_PLATFORM,
                          source=self.bucket, location=blob_name,
                          position=_START_POSITION_IN_BLOB,
                          status=blob.BlobStatus.ERROR, status_desc=str(error))
