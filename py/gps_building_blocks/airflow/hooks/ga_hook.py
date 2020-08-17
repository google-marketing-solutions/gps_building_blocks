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

import re
import sys
import time
from typing import Any, Dict, List, Mapping, Optional, Text, Tuple
import urllib.parse
import urllib.request

from googleapiclient import errors as googleapiclient_errors

from gps_building_blocks.airflow.hooks import output_hook_interface
from gps_building_blocks.airflow.utils import errors
from gps_building_blocks.airflow.utils import retry_utils

_HIT_TYPES = ('pageview', 'screenview', 'event', 'transaction', 'item',
              'social', 'exception', 'timing')
_PAYLOAD_VERSION = '1'

_BATCH_MAX_BATCH_LENGTH = 20
_BATCH_MAX_BATCH_SIZE_BYTES = 16384  # 2**14
_SINGLE_MAX_PAYLOAD_SIZE_BYTES = 8192
_GA_TRACKING_ID_REGEX = r'^UA-\d{5,}-\d+$'
_BASE_URL_ENDPOINT = 'https://www.google-analytics.com'
_SEND_TYPE_MAPPING = {'single': 'collect', 'batch': 'batch'}


class PayloadBuilder(object):
  """Payload Builder, generate payload based on hit type."""

  def __init__(self, tracking_id: Text) -> None:
    """Checks if tracking number is valid and initializes Payload Builder.

    Args:
      tracking_id: GA's property or tracking ID.
    """
    self.tracking_id = tracking_id

  def _validate_hit_type(self, hit_type: Text) -> None:
    """Validates if hit type is supported.

    The input hit type must be in the supported list _HIT_TYPES.

    Args:
      hit_type: The hit type to check.

    Raises:
      DataOutConnectorValueError: If the hit type is not in _HIT_TYPES.
    """

    if hit_type not in _HIT_TYPES:
      raise errors.DataOutConnectorValueError(
          'Unsupported hit type %s was detected. Supported types are: %s.' % (
              hit_type, _HIT_TYPES))

  def _validate_uid_or_cid(self, cid: Optional[Text],
                           uid: Optional[Text]) -> None:
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
      raise errors.DataOutConnectorValueError('Hit must have cid or uid.')

  def _validate_batch_max_size(self, params: List[Mapping[Text, Any]]) -> None:
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
          'Batch hits must be under 20.')

  def _validate_payload_size(self, payload: Text, send_type: Text) -> None:
    """Validates hit size does not exceed max size.

    Single hit payload size must be under 8192 bytes, batch hit payload size
    must be under 16384 bytes.

    Args:
      payload: Urlencoded payload string.
      send_type: The hit request type; can be 'single' or 'batch'.

    Raises:
      DataOutConnectorInvalidPayloadError: If the payload exceeds the size
      limitation.
    """
    limit_size = (_SINGLE_MAX_PAYLOAD_SIZE_BYTES if send_type == 'single' else
                  _BATCH_MAX_BATCH_SIZE_BYTES)
    payload_size = sys.getsizeof(payload)
    if payload_size > limit_size:
      raise errors.DataOutConnectorInvalidPayloadError(
          'Hit size %s exceeds limitation %s.' % (payload_size, limit_size))

  def generate_single_payload(self, hit_type: Text,
                              payload_contents: Mapping[Text, Any]
                             ) -> Text:
    """Generates single payload.

    Generates single payload to be sent to GA.

    Args:
      hit_type: One of the hit types in _HIT_TYPES.
      payload_contents: Hit payload contents.

    Returns:
      Url encoded payload string.
    """
    self._validate_hit_type(hit_type)
    self._validate_uid_or_cid(payload_contents.get('cid', None),
                              payload_contents.get('uid', None))
    payload = {
        'tid': self.tracking_id,
        'v': _PAYLOAD_VERSION,
        't': hit_type,
        'z': int(time.time() * 10**6)
    }
    payload.update(payload_contents)
    payload_str = urllib.parse.urlencode(payload)
    self._validate_payload_size(payload_str, 'single')
    return payload_str

  def generate_batch_payload(self, hit_type: Text,
                             payload_contents: List[Mapping[Text, Any]]
                            ) -> Text:
    """Generates batch payload.

    Batch payload will compose the payloads that generated by single_payload
    in multiple line format in one payload.

    Args:
      hit_type: One of the hit types in _HIT_TYPES.
      payload_contents: each dict obj must include (cid or uid), plus
          all contents required for that hit type.
          [
          {'ev':1, 'ec':'action', 'cid':'11.111'},
          {'ev':2, 'ec':'action', 'cid':'22.222'},
          ]

    Returns:
      Url encoded payload string.
    """
    self._validate_batch_max_size(payload_contents)

    payload_text = '\n'.join([
        self.generate_single_payload(hit_type, pc) for pc in payload_contents])
    self._validate_payload_size(payload_text, 'batch')

    return payload_text


class GoogleAnalyticsHook(output_hook_interface.OutputHookInterface):
  """Custom hook for GA via Measurement Protocol API."""

  def __init__(self,
               tracking_id: Text,
               base_params: Optional[Mapping[Text, Any]] = None,
               dry_run: Optional[bool] = False) -> None:
    """Initializes the class.

    Creates a GAMeasurementProtocolHook for use across all requests.

    Args:
      tracking_id: Google Analytics' tracking id to identify a property.
      base_params: Default parameters that serve as the base on which to build
        the Measurement Protocol payload.
      dry_run: If True, this will not send real hits to the endpoint.
    """
    self._validate_tracking_id(tracking_id)
    self.tracking_id = tracking_id
    self.dry_run = dry_run

    if base_params:
      self.base_params = base_params
    else:
      self.base_params = {}

  def _validate_tracking_id(self, tracking_id: Text) -> None:
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
          'Invalid Tracking ID Format. The expected format is `UA-XXXXX-Y`.')

  def _validate_send_type(self, send_type: Text) -> None:
    """Validates send type.

    Only accept 'single' or 'batch' as send_type.

    Args:
      send_type: The hit request type; can be 'single' or 'batch'.

    Raises:
        DataOutConnectorSendUnsuccessfulError: If the send type is neither
        single nor batch.
    """
    if send_type not in _SEND_TYPE_MAPPING.keys():
      raise errors.DataOutConnectorValueError(msg=(
          'Wrong send type.'
          ' Expected single or batch but received %s instead' % send_type))

  @retry_utils.logged_retry_on_retriable_http_error
  def _send_http_request(self, data: Text, url: Text, header: Dict[Text, Text]
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

  def _get_hit_url(self, send_type: Text) -> Text:
    """Gets full url path for sending the hit.

    Args:
      send_type: The hit request type; can be 'single' or 'batch'.

    Returns:
      Full url path of sending hit.
    """
    return '/'.join([_BASE_URL_ENDPOINT, _SEND_TYPE_MAPPING[send_type]])

  def _get_next_batch_payload(self,
                              events_queue: List[Tuple[int, Dict[Any, Any]]],
                              builder: PayloadBuilder
                             ) -> Tuple[List[Text], List[int], List[int]]:
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
      events_queue: Enumerated events to send to GA.
      builder: A PayloadBuilder used to generate the actual payload.

    Returns:
      A list of payloads, where each element belongs to a single event.
      A list of  indixes of events that were appended to the payload.
      A list of indexes of events that were oversized or missing required
      fields.
    """
    payload = []
    events_in_payload_indexes = []
    erroneous_events_indexes = []
    payload_size = 0

    while events_queue:
      event = events_queue[0]
      payload_dict = {**self.base_params, **event[1]}

      try:
        event_payload = builder.generate_single_payload('event', payload_dict)
      except (errors.DataOutConnectorInvalidPayloadError,
              errors.DataOutConnectorValueError):
        erroneous_events_indexes.append(event[0])
        events_queue.pop(0)
        continue

      event_size = sys.getsizeof(event_payload)

      # If adding another event exceeds payload limits return the payload as is.
      if (payload_size + event_size > _BATCH_MAX_BATCH_SIZE_BYTES or
          len(payload) + 1 > _BATCH_MAX_BATCH_LENGTH):
        return payload, events_in_payload_indexes, erroneous_events_indexes

      payload_size += event_size
      payload.append(event_payload)
      events_in_payload_indexes.append(event[0])
      events_queue.pop(0)

    return payload, events_in_payload_indexes, erroneous_events_indexes

  def send_hit(self, payload: Text,
               user_agent: Text = '',
               send_type: Text = 'single') -> None:
    """Sends hit via measurement protocol.

    Sends hit based on payload data.

    Args:
      payload: Contains payload data for hit.
      user_agent: User agent string injected in http request header.
      send_type: The hit request type; can be 'single' or 'batch'.

    Raises:
      DataOutConnectorSendUnsuccessfulError: If sending a hit to GA has failed.
    """
    self._validate_send_type(send_type)

    url = self._get_hit_url(send_type)

    if self.dry_run:
      self.log.info('Dry Run Mode: Skipped sending the hit to GA.')
      return

    headers = {'User-Agent': user_agent} if user_agent else {}
    try:
      self._send_http_request(payload, url, headers)
    except googleapiclient_errors.HttpError as error:
      raise errors.DataOutConnectorSendUnsuccessfulError(
          error=error, msg='Sending a hit to GA has completed unsuccessfully.')

  def send_events(self, events: List[Dict[Any, Any]]
                 ) -> Tuple[List[int], List[int]]:
    """Sends all events in the queue to the GA API, (FIFO, via batch).

    Args:
      events: Events to send to GA.

    Returns:
      Tuple of a list of indexes of successfully sent events, and a list of
      indexes of unsuccessfully sent events.

    """
    builder = PayloadBuilder(self.tracking_id)
    successfully_sent_event_indexes = []
    unsuccessfully_sent_event_indexes = []

    # Enumerate the list for returning which events were successfully sent by
    # their indexes.
    events = [(i, event) for i, event in enumerate(events)]

    while events:
      (next_batch_payload,
       events_in_payload_indixes,
       erroneous_events_indexes) = self._get_next_batch_payload(events, builder)
      unsuccessfully_sent_event_indexes.extend(erroneous_events_indexes)

      if next_batch_payload:
        try:
          self.send_hit('\n'.join(next_batch_payload), send_type='batch')
          successfully_sent_event_indexes.extend(events_in_payload_indixes)
        except (errors.DataOutConnectorSendUnsuccessfulError,
                errors.DataOutConnectorValueError):
          unsuccessfully_sent_event_indexes.extend(events_in_payload_indixes)

    return (successfully_sent_event_indexes, unsuccessfully_sent_event_indexes)
