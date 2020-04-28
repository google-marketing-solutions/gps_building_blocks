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

"""Custom Hook for Datastore."""

import datetime

from typing import Any, Dict, Generator, Mapping, Text
from airflow.contrib.hooks import datastore_hook
from googleapiclient import errors as googleapiclient_errors

from gps_building_blocks.cc4d.utils import blob
from gps_building_blocks.cc4d.utils import errors
from gps_building_blocks.cc4d.utils import retry_utils


_ENTITY = 'blob_entity'

_COMMIT_METHOD_INSERT = 'insert'
_COMMIT_METHOD_UPDATE = 'update'


class DatastoreHook(datastore_hook.DatastoreHook):
  """Custom hook that manages blobs' status in Datastore."""

  def _get_current_timestamp_in_datastore_format(self):
    """Generates current timestamp in Datastore format."""
    return datetime.datetime.utcnow().isoformat() + 'Z'

  def _build_body_for_commit(self, blb: blob.Blob, method: Text
                            ) -> Dict[Text, Any]:
    """Builds request body for commit method.

    Args:
    　blb: Blob object containing blob info.
      method: commit method.

    Returns:
      Request body for commit method.
    """
    body = {
        'mode':
            'NON_TRANSACTIONAL',
        'mutations': [{
            method: {
                'key': {
                    'path': [{
                        'kind': _ENTITY,
                        'name': blb.blob_id
                    }]
                },
                'properties': {
                    'timestamp': {
                        'timestampValue':
                            self._get_current_timestamp_in_datastore_format()
                    },
                    'platform': {'stringValue': blb.platform},
                    'source': {'stringValue': blb.source},
                    'location': {'stringValue': blb.location},
                    'position': {'integerValue': blb.position},
                    'status': {'stringValue': blb.status.name},
                    'status_desc': {'stringValue': blb.status_desc},
                    'num_events': {'integerValue': len(blb.events)},
                }
            }
        }]
    }
    return body

  def _build_body_for_get_all_blobs_with_status_query(self, source: Text,
                                                      status: blob.BlobStatus
                                                     ) -> Dict[Text, Any]:
    """Builds request body for run query method.

    Args:
      source: The source to get the blobs from.
      status: processing status of the file.

    Returns:
      Request body for run query method.
    """

    body = {
        'query': {
            'filter': {
                'compositeFilter': {
                    'filters': [{
                        'propertyFilter': {
                            'property': {
                                'name': 'source'
                            },
                            'op': 'EQUAL',
                            'value': {
                                'stringValue': source
                            }
                        }
                    }, {
                        'propertyFilter': {
                            'property': {
                                'name': 'status'
                            },
                            'op': 'EQUAL',
                            'value': {
                                'stringValue': status.name
                            }
                        }
                    }],
                    'op': 'AND'
                }
            },
            'kind': [{
                'name': _ENTITY
            }]
        }
    }
    return body

  def _parse_blob_from_response_entity(self, entity: Dict[Text, Any]
                                      ) -> blob.Blob:
    """Parses response entity of run query API call to get blob information.

    Args:
      entity: Response entity of run query API call containing blob information.

    Returns:
      A blob object (with no events) constructed from the Datastore response.
    """
    blob_id = entity.get(
        'entity', {}).get('key', {}).get('path', [{}])[0].get('name', '')
    properties = entity.get('entity', {}).get('properties', {})
    platform = properties.get('platform', {}).get('stringValue', '')
    source = properties.get('source', {}).get('stringValue', '')
    location = properties.get('location', {}).get('stringValue', '')
    position = properties.get('position', {}).get('integerValue', 0)
    status = properties.get('status', {}).get('stringValue', '')
    status = blob.BlobStatus[status] if status else blob.BlobStatus.ERROR
    status_desc = properties.get('status_desc', {}).get('stringValue', '')
    num_events = properties.get('num_events', {}).get('integerValue', None)

    return blob.Blob(events=[], blob_id=blob_id, platform=platform,
                     source=source, location=location, position=position,
                     status=status, status_desc=status_desc,
                     num_events=num_events)

  @retry_utils.logged_retry_on_retriable_http_error
  def commit_with_retries_on_retriable_http_error(
      self, body: Dict[Text, Any]) -> Dict[Text, Any]:
    """Commits to Datastore with retries.

    Args:
      body: Request body.
    Returns:
      Response of Datastore API request.
    """
    return self.commit(body=body)

  def insert_blob_information(self, blb: blob.Blob) -> Dict[Text, Any]:
    """Inserts blob information to Datastore.

    Args:
      blb: The blob object containing the information to update in Datastore.

    Returns:
      Response of Datastore API request.

    Raises:
      DatastoreInsertBlobInfoError: raised when insert commit returns an error.
    """
    body = self._build_body_for_commit(blb, _COMMIT_METHOD_INSERT)
    try:
      response = self.commit_with_retries_on_retriable_http_error(body=body)
    except googleapiclient_errors.HttpError as error:
      raise errors.DatastoreInsertBlobInfoError(
          msg='Failed to insert blob status in Datastore', error=error)
    return response

  def update_blob_information(self, blb: blob.Blob) -> Dict[Text, Any]:
    """Updates blob information.

    Args:
      blb: The blob object containing the information to update in Datastore.

    Returns:
      Response of Datastore API request.

    Raises:
      DatastoreUpdateBlobStatusError: raised when update commit returns an
      error.
    """
    body = self._build_body_for_commit(blb, _COMMIT_METHOD_UPDATE)
    try:
      response = self.commit_with_retries_on_retriable_http_error(body=body)
    except googleapiclient_errors.HttpError as error:
      raise errors.DatastoreUpdateBlobInfoError(
          msg='Failed to update blob status in Datastore', error=error)
    return response

  def insert_or_update_blob_information(self, blb: blob.Blob
                                       ) -> Dict[Text, Any]:
    """Inserts or updates blob information in Datastore.

    Data store does Overrides an entity if it exists.
    The function would try to insert the blob into Datastore, then will try to
    update it in case insertion failed due to existing entity key.

    Args:
      blb: The blob object containing the information to update in Datastore.

    Returns:
      Response of Datastore API request.
    """
    try:
      response = self.insert_blob_information(blb)
    except errors.DatastoreInsertBlobInfoError:
      response = self.update_blob_information(blb)
    return response

  @retry_utils.logged_retry_on_retriable_http_error
  def run_query_with_retries_on_retriable_http_error(self,
                                                     body: Dict[Text, Any]
                                                    ) -> Mapping[Text, Any]:
    """Runs query with retries from Datastore.

    Args:
      body: Request body.

    Returns:
      Query results
    """
    return self.run_query(body=body)

  def generate_blobs_with_status(self, source: Text, status: blob.BlobStatus
                                ) -> Generator[blob.Blob, None, None]:
    """Generates all blobs from source that are in given status from Datastore.

    TODO(saraid): Paginate datastore results in this method.

    Args:
      source: The source of the wanted blobs.
      status: The status of blobs to get.

    Yields:
      A generator that generates Blob objects from blob contents in datastore.

    Raises:
      DatastoreRunQueryError: Raised when run_query returns an error.
    """
    body = self._build_body_for_get_all_blobs_with_status_query(source, status)
    try:
      response = self.run_query_with_retries_on_retriable_http_error(body=body)
    except googleapiclient_errors.HttpError as error:
      raise errors.DatastoreRunQueryError(
          msg='Failed while getting blobs from Datastore', error=error)

    entity_results = response.get('entityResults', [])
    for entity in entity_results:
      yield self._parse_blob_from_response_entity(entity)
