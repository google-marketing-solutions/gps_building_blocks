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

"""Custom Hook for Google Analytics."""

import enum
import re
import sys
import time
from typing import Any, Dict, List, Optional, Tuple, Generator
import urllib.parse
import urllib.request

from googleapiclient import errors as googleapiclient_errors

from gps_building_blocks.tcrm.hooks import output_hook_interface
from gps_building_blocks.tcrm.utils import blob
from gps_building_blocks.tcrm.utils import errors
from gps_building_blocks.tcrm.utils import retry_utils

_PAYLOAD_VERSION = '1'

_BATCH_MAX_BATCH_LENGTH = 20
_BATCH_MAX_BATCH_SIZE_BYTES = 16384  # 2**14
_SINGLE_MAX_PAYLOAD_SIZE_BYTES = 8192
_GA_TRACKING_ID_REGEX = r'^UA-\d{5,}-\d+$'
_BASE_URL_ENDPOINT = 'https://www.google-analytics.com'


class HitTypes(enum.Enum):
  """Supported Google Analytics hit types."""

  EVENT = 'event'
  PAGEVIEW = 'pageview'
  SCREENVIEW = 'screenview'
  TRANSACTION = 'transaction'
  ITEM = 'item'
  SOCIAL = 'social'
  EXCEPTION = 'exception'
  TIMING = 'timing'


class SendTypes(enum.Enum):
  """Supported Google Analytics send types."""

  SINGLE = 'collect'
  BATCH = 'batch'


class PayloadBuilder(object):
  """Payload Builder, generate payload based on hit type."""

  def __init__(self, tracking_id: str) -> None:
    """Checks if tracking number is valid and initializes Payload Builder.

    Args:
      tracking_id: GA's property or tracking ID.
    """
    self.tracking_id = tracking_id

  def _validate_uid_or_cid(self, cid: Optional[str],
                           uid: Optional[str]) -> None:
    """Validates uid or cid.

    Each payload must include cid (client id) or uid (user id) in it; this
    function verifies either uid or cid are set.

    Args:
      cid: Client id to check.
      uid: User id to check.

    Raises:
      DataOutConnectorValueError: If input parameter didn't cover either cid or
      uid.
    """
    if not cid and not uid:
      raise errors.DataOutConnectorValueError(
          'Hit must have cid or uid.',
          error_num=errors.ErrorNameIDMap.GA_HOOK_ERROR_MISSING_CID_OR_UID)

  def _validate_batch_max_size(self, params: List[Dict[str, Any]]) -> None:
    """Validates batch hits not exceed max size.

    Based on the developer guide of GA measurement protocol,
    (https://developers.google.com/analytics/devguides/collection/protocol/v1/devguide)
    one batch can contain up to 20 hits.

    Args:
      params: Payload params.

    Raises:
      DataOutConnectorInvalidPayloadError: If the payload contains more than
      20 hits.
    """
    if len(params) > _BATCH_MAX_BATCH_LENGTH:
      raise errors.DataOutConnectorInvalidPayloadError(
          'Batch hits must be under 20.',
          errors.ErrorNameIDMap.GA_HOOK_ERROR_BATCH_LENGTH_EXCEEDS_LIMITATION)

  def _validate_payload_size(self, payload: str, send_type: SendTypes) -> None:
    """Validates hit size does not exceed max size.

    Single hit payload size must be under 8192 bytes, batch hit payload size
    must be under 16384 bytes.

    Args:
      payload: Urlencoded payload string.
      send_type: The hit request type from SendTypes..

    Raises:
      DataOutConnectorInvalidPayloadError: If the payload exceeds the size
      limitation.
    """
    limit_size = (_SINGLE_MAX_PAYLOAD_SIZE_BYTES if send_type ==
                  SendTypes.SINGLE else _BATCH_MAX_BATCH_SIZE_BYTES)
    payload_size = sys.getsizeof(payload)
    print(send_type, limit_size, payload_size)
    if payload_size > limit_size:
      print('*'*60)
      raise errors.DataOutConnectorInvalidPayloadError(
          'Hit size %s exceeds limitation %s.' % (payload_size, limit_size),
          errors.ErrorNameIDMap.GA_HOOK_ERROR_HIT_SIZE_EXCEEDS_LIMITATION)

  def generate_single_payload(self, hit_type: HitTypes, event: Dict[str, Any],
                              base_params: Dict[str, Any] = None) -> str:
    """Generates single payload to be sent to GA.

    Args:
      hit_type: One of the hit types in HitType.
      event: event to send.
      base_params: Google Analytics base parameters for sending event.

    Returns:
      Url encoded payload string.
    """
    if base_params is None:
      base_params = {}
    payload_dict = {**base_params, **event}

    self._validate_uid_or_cid(payload_dict.get('cid', None),
                              payload_dict.get('uid', None))

    payload = {
        'tid': self.tracking_id,
        'v': _PAYLOAD_VERSION,
        't': hit_type.value,
        'z': int(time.time() * 10**6)
    }

    payload.update(payload_dict)
    payload_str = urllib.parse.urlencode(payload)
    self._validate_payload_size(payload_str, SendTypes.SINGLE)

    return payload_str

  def generate_batch_payload(self, hit_type: HitTypes,
                             events: List[Dict[str, Any]],
                             base_params: Dict[str, Any] = None) -> str:
    """Generates batch payload.

    Batch payload will generate multiple lines hits in one payload.

    Args:
      hit_type: One of the hit types in _HIT_TYPES.
      events: A list of JSON events to convert.
      base_params: Default parameters that serve as the base on which to
        build the Measurement Protocol payload.

    Returns:
      Url encoded payload string.
    """
    self._validate_batch_max_size(events)
    if base_params is None:
      base_params = {}

    payloads = []
    for event in events:
      payloads.append(self.generate_single_payload(
          hit_type, event, base_params))

    payload_str = '\n'.join(payloads)

    self._validate_payload_size(payload_str, SendTypes.BATCH)

    return payload_str


class GoogleAnalyticsHook(output_hook_interface.OutputHookInterface):
  """Custom hook for GA via Measurement Protocol API."""

  def __init__(self,
               ga_tracking_id: str,
               ga_base_params: Optional[Dict[str, Any]] = None,
               ga_dry_run: Optional[bool] = False,
               **kwargs) -> None:
    """Initializes the class.

    Creates a GAMeasurementProtocolHook for use across all requests.

    Args:
      ga_tracking_id: Google Analytics' tracking id to identify a property.
      ga_base_params: Default parameters that serve as the base on which to
        build the Measurement Protocol payload.
      ga_dry_run: If True, this will not send real hits to the endpoint.
      **kwargs: Other optional arguments.
    """
    self._validate_tracking_id(ga_tracking_id)
    self.tracking_id = ga_tracking_id
    self.dry_run = ga_dry_run

    if ga_base_params:
      self.base_params = ga_base_params
    else:
      self.base_params = {}

  def _validate_tracking_id(self, tracking_id: str) -> None:
    """Validates tracking matches the common pattern.

    The tracking id must comply the specified pattern 'UA-XXXXX-Y' to proceed
    the send_hit function.

    Args:
      tracking_id: GA's property or tracking ID for GA to identify hits.

    Raises:
      DataOutConnectorValueError: If the tracking id format is invalid.
    """
    if not re.match(_GA_TRACKING_ID_REGEX, tracking_id):
      raise errors.DataOutConnectorValueError(
          'Invalid Tracking ID Format. The expected format is `UA-XXXXX-Y`.',
          errors.ErrorNameIDMap.GA_HOOK_ERROR_INVALID_TRACKING_ID_FORMAT)

  @retry_utils.logged_retry_on_retriable_http_error
  def _send_http_request(self, data: str, url: str, header: Dict[str, str]
                        ) -> None:
    """Sends a http request.

    Args:
      data: Contains payload data for hit.
      url: Target url.
      header: Http headers.

    Raises:
      HttpError: If sending failed.
    """
    request = urllib.request.Request(url,
                                     data=data.encode('utf-8'),
                                     headers=header)
    request.add_header('Content-Type',
                       'application/x-www-form-urlencoded; charset=UTF-8')
    with urllib.request.urlopen(request) as response:
      if int(response.status / 100) != 2:
        raise googleapiclient_errors.HttpError(response, b'', url)

  def _get_hit_url(self, send_type: SendTypes) -> str:
    """Gets full url path for sending the hit.

    Args:
      send_type: The hit request type; can be 'single' or 'batch'.

    Returns:
      Full url path of sending hit.
    """
    return '/'.join([_BASE_URL_ENDPOINT, send_type])

  def _batch_generator(self, events: List[Tuple[int, Dict[Any, Any], str]]
                      ) -> Generator[str, None, None]:
    """Gets a list of payloads from the next batch to be sent.

    # v1 API Batch Limitations:
    # (https://developers.google.com/analytics/devguides/
    #  collection/protocol/v1/devguide#batch-limitations)

    Batches are limited in size; the batch will cut off if adding
    the next element to the batch would violate any of the following
    conditions:
      - The next element would be None (the queue is now empty).
      - The batch would be larger than _BATCH_MAX_BATCH_LENGTH.
      - The batch would have more bytes than is allowed in a batch.
      - The next element is too large to include in a batch.

    Args:
      events: index-event-payload tuples list of events to send to GA.

    Yields:
      A batch payload, where each element in it belongs to a single event.

    """
    payload = []
    payload_size = 0

    for event in events:
      event_size = sys.getsizeof(event[2])

      if (payload_size + event_size > _BATCH_MAX_BATCH_SIZE_BYTES or
          len(payload) + 1 > _BATCH_MAX_BATCH_LENGTH):
        yield '\n'.join(payload)
        payload = []
        payload_size = 0

      payload_size += event_size
      payload.append(event[2])

    if payload:
      yield '\n'.join(payload)

  def _validate_and_prepare_events_to_send(
      self, events: List[Dict[str, Any]],
      hit_type: HitTypes) -> Tuple[List[Tuple[int, Dict[str, Any], str]],
                                   List[Tuple[int, errors.ErrorNameIDMap]]]:
    """Prepares index-event tuples to keep order while sending.

    Args:
      events: Events to prepare for sending.
      hit_type: The type of hit to send per HitTypes.

    Returns:
      A list of index-event tuples for the valid events, and a list of
      index-error for the invalid events.
    """
    valid_events = []
    invalid_indices_and_errors = []

    builder = PayloadBuilder(self.tracking_id)

    for i, event in enumerate(events):
      try:
        event_payload = builder.generate_single_payload(hit_type, event,
                                                        self.base_params)
      except (errors.DataOutConnectorInvalidPayloadError,
              errors.DataOutConnectorValueError) as error:
        invalid_indices_and_errors.append((i, error.error_num))
      else:
        valid_events.append((i, event, event_payload))

    return valid_events, invalid_indices_and_errors

  def send_hit(self, payload: str, user_agent: str = '',
               send_type: SendTypes = SendTypes.SINGLE) -> None:
    """Sends hit via measurement protocol.

    Sends hit based on payload data.

    Args:
      payload: Contains payload data for hit.
      user_agent: User agent string injected in http request header.
      send_type: The hit request type; can be 'single' or 'batch'.

    Raises:
      DataOutConnectorSendUnsuccessfulError: If sending a hit to GA has failed.
    """
    url = self._get_hit_url(send_type.value)

    if self.dry_run:
      self.log.info('Dry Run Mode: Skipped sending the hit to GA.')
      return

    headers = {'User-Agent': user_agent} if user_agent else {}
    try:
      self._send_http_request(payload, url, headers)
    except googleapiclient_errors.HttpError as error:
      raise errors.DataOutConnectorSendUnsuccessfulError(
          error=error, msg='Sending a hit to GA has completed unsuccessfully.',
          error_num=errors.ErrorNameIDMap.RETRIABLE_GA_HOOK_ERROR_HTTP_ERROR)

  def send_events(self, blb: blob.Blob) -> blob.Blob:
    """Sends all events in the blob to the GA API.

    Args:
      blb: A blob containing Customer Match data to send.

    Returns:
      A blob containing updated data about any failing events or reports.

    """
    valid_events, invalid_indices_and_errors = \
        self._validate_and_prepare_events_to_send(blb.events, HitTypes.EVENT)

    batches = self._batch_generator(valid_events)

    for batch in batches:
      try:
        self.send_hit(batch, send_type=SendTypes.BATCH)
      except (errors.DataOutConnectorSendUnsuccessfulError,
              errors.DataOutConnectorValueError) as error:
        for event in batch:
          invalid_indices_and_errors.append((event[0], error.error_num))

    for event in invalid_indices_and_errors:
      blb.append_failed_event(event[0] + blb.position, blb.events[event[0]],
                              event[1].value)

    return blb
