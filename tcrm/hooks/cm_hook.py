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

"""Custom hook for Campaign Manager API.

Custom Airflow hook that sends conversion events to Campaign Manager
batch insert API.

API Documentation:
https://developers.google.com/doubleclick-advertisers/v3.3/conversions/batchinsert#request-body

This module consists of two classes:

PayloadBuilder: Generates conversion payload list to send to the API.
CMHook: Generates, validates and sends the conversion data to the API.
  It overrides the output_hook_interface.OutputHook's send_events method.
  This method initiates validation, batching and send of the
  conversion data to the API.
"""

import re
import time
from typing import Any, Dict, Generator, List, Tuple

from gps_building_blocks.cloud.utils import cloud_auth
from gps_building_blocks.tcrm.hooks import output_hook_interface
from gps_building_blocks.tcrm.utils import blob
from gps_building_blocks.tcrm.utils import errors

_CONVERSION_REQUIRED_FIELDS = ('gclid',
                               'floodlightActivityId',
                               'floodlightConfigurationId',
                               'ordinal',
                               'timestampMicros',
                               'quantity',
                               'value')

_CONVERSION_OPTIONAL_FIELDS = ('childDirectedTreatment',
                               'limitAdTracking',
                               'nonPersonalizedAd',
                               'treatmentForUnderage',
                               'customVariables')
_CONVERSION_BATCH_MAX_SIZE = 1000
_MAX_RETRIES = 5

_API_SERVICE = 'dfareporting'
_API_SCOPE = 'https://www.googleapis.com/auth/ddmconversions'
_API_VERSION = 'v3.3'


# When following types of service side error happen, sending would be retried.
# https://developers.google.com/doubleclick-advertisers/v3.3/conversions/batchinsert#response
_RETRIABLE_ERRORS = ('INTERNAL',
                     'PERMISSION_DENIED',
                     'NOT_FOUND')


class PayloadBuilder(object):
  """Generates conversion operation list to send to Campaign Manager."""

  def _validate_required_fields(self, event: Dict[str, Any]) -> None:
    """Validates all required fields are present in the event JSON.

    Validates the event contains all the fields in
    _CONVERSION_REQUIRED_FIELDS.

    Args:
      event: Offline Conversion JSON event.

    Raises:
      DataOutConnectorInvalidPayloadError: If any required field is missing.
    """
    for field in _CONVERSION_REQUIRED_FIELDS:
      if field not in event.keys():
        raise errors.DataOutConnectorInvalidPayloadError(
            'Event is missing mandatory field {}'.format(field),
            errors.ErrorNameIDMap.CM_HOOK_ERROR_MISSING_MANDATORY_FIELDS)

  def _is_valid_custom_variable_type(self, custom_variable_type: str) -> bool:
    """Validates the custom variable type matches pattern U1-U100."""
    pattern = 'U[1-9]|U[1-9][0-9]|U100'
    return re.fullmatch(pattern, custom_variable_type) is not None

  def _validate_custom_variables(self, event: Dict[str, Any]) -> None:
    """Validates the fields in the custom floodlight variables list.

    Validates the "customVariables" field conforms to the limitations
    mentioned in the API doc.
    https://developers.google.com/doubleclick-advertisers/v3.3/conversions#resource

    Custom Variables should be a list of the following format:

    "customVariables": [
      {
        "kind": "dfareporting#customFloodlightVariable",
        "type": string,
        "value": string
      }
    ]

    "kind": Fixed value equal to "dfareporting#customFloodlightVariable".
    "type":  The type of  custom floodLight variable. It maps to U[1-100].
    "value": The value of the custom floodlight variable with max 50 characters.

    Args:
      event: Offline Conversion JSON event.

    Raise:
      DataOutConnectorInvalidPayloadError: If the format of any item in the
      "customVariables" list is invalid.
    """
    custom_variables = event.get('customVariables')
    if custom_variables is not None:
      for item in custom_variables:
        if not all([item['kind'] == 'dfareporting#customFloodlightVariable',
                    self._is_valid_custom_variable_type(item['type']),
                    len(item['value']) <= 50]):
          raise errors.DataOutConnectorInvalidPayloadError(
              '{} in customVariables list is invalid.'.format(item),
              errors.ErrorNameIDMap
              .CM_HOOK_ERROR_INVALID_VALUE_IN_CUSTOM_VARIABLES)

  def generate_single_payload(self,
                              event: Dict[str, Any]) -> Dict[str, Any]:
    """Generates a single payload event JSON object with validation."""
    self._validate_required_fields(event)
    self._validate_custom_variables(event)
    conversion = {}
    for field in _CONVERSION_REQUIRED_FIELDS:
      conversion[field] = event[field]
    for field in set(_CONVERSION_OPTIONAL_FIELDS).intersection(
        event.keys()):
      conversion[field] = event[field]
    return conversion


class CampaignManagerHook(output_hook_interface.OutputHookInterface):
  """Custom hook for Campaign Manager API."""

  def __init__(self, cm_service_account: str,
               cm_profile_id: str, **kwargs) -> None:
    """Initializes the Campaign Manager hook.

    Args:
      cm_service_account: Name of service account authenticated as CM user.
      cm_profile_id: User profile id of the Campaign Manager user account.
      **kwargs: Other optional arguments.
    Raises:
      ValueError if service_account or profile_id is empty.
    """
    if not cm_service_account:
      raise ValueError('Empty service_account arg not allowed!')

    if not cm_profile_id:
      raise ValueError('Empty profile_id arg not allowed!')

    self._cm_service = cloud_auth.build_impersonated_client(
        _API_SERVICE, cm_service_account, _API_VERSION, _API_SCOPE)
    self._profile_id = cm_service_account

  def _validate_and_prepare_events_to_send(
      self, events: List[Dict[str, Any]]
      ) -> Tuple[List[Tuple[int, Dict[str, Any]]],
                 List[Tuple[int, errors.ErrorNameIDMap]]]:
    """Prepares index-event tuples to keep order while sending.

    Args:
      events: Events to prepare for sending.

    Returns:
      A list of index-event tuples for the valid events, and a list of
      index-error for the invalid events.
    """
    builder = PayloadBuilder()
    valid_events = []
    invalid_indices_and_errors = []

    for i, event in enumerate(events):
      try:
        payload = builder.generate_single_payload(event)
      except errors.DataOutConnectorInvalidPayloadError as error:
        invalid_indices_and_errors.append((i, error.error_num))
      else:
        valid_events.append((i, payload))

    return valid_events, invalid_indices_and_errors

  def _batch_generator(
      self, events: List[Tuple[int, Dict[str, Any]]]
      ) -> Generator[List[Tuple[int, Dict[str, Any]]], None, None]:
    """Splits conversion events into batches of _CONVERSION_BATCH_MAX_SIZE.

    Args:
      events: Indexed events to send.

    Yields:
      List of batches of events. Each batch is of _CONVERSION_BATCH_MAX_SIZE.
    """
    for i in range(0, len(events), _CONVERSION_BATCH_MAX_SIZE):
      yield events[i : i + _CONVERSION_BATCH_MAX_SIZE]

  def _create_request(self, events: List[Dict[str, Any]]):
    """Creates request body for the API call."""
    request_body = {
        'kind': 'dfareporting#conversionsBatchInsertRequest',
        'conversions': list(events),
    }
    return request_body

  def _is_retriable_error(self, status_errors: List[Dict[str, Any]]) -> bool:
    """Checks if one of the errors in the response status is retriable.

    See error code values in the response object in the link below.
    https://developers.google.com/doubleclick-advertisers/v3.3/conversions/batchinsert#response

    Args:
      status_errors: Response from the API on the insert status of a conversion.
        Contains error codes, conversion object and detailed error message.

    Returns:
      True if any one of the errors is in _RETRIABLE_ERRORS.
    """
    if status_errors is not None:
      for error in status_errors:
        if 'code' in error and error['code'] not in _RETRIABLE_ERRORS:
          return False
    return True

  def _extract_failed_events_info_from_response(self, response: Dict[str, Any],
                                                batch_size: int
                                               ) -> Tuple[List[int], List[str]]:
    """Extracts failed event indices and reasons from Ads API response object.

    Args:
      response: Response object from the conversion API.
        https://developers.google.com/doubleclick-advertisers/v3.3/conversions/batchinsert#response_1
      batch_size: The number of events in the response.

    Returns:
      Tuple of error indices and error reasons.
    """
    error_indices = []
    reasons = []

    if 'status' in response:
      for i, status in enumerate(response['status']):
        if 'errors' in status and status['errors']:
          error_indices.append(i)
          if self._is_retriable_error(status['errors']):
            reasons.append(errors.ErrorNameIDMap.RETRIABLE_ERROR_EVENT_NOT_SENT)
          else:
            reasons.append(
                errors.ErrorNameIDMap.NON_RETRIABLE_ERROR_EVENT_NOT_SENT)
    else:
      error_indices.extend(range(batch_size))
      reasons.extend(
          [errors.ErrorNameIDMap.NON_RETRIABLE_ERROR_EVENT_NOT_SENT
           for i in range(batch_size)])

    return error_indices, reasons

  def _send_batch(self, batch: List[Tuple[int, Dict[str, Any]]]
                 ) -> List[Tuple[int, errors.ErrorNameIDMap]]:
    """Calls the CM API to send the batch of conversions.

    Retries sending in 2 cases:
    1. If there is a partial failure wherein some conversions
    are sent and some are not due to INTERNAL error as per the API response.
    2. If there is a retriable HTTP error raised during the API call.

    Args:
      batch: A dict of events, the key of the dict is the index of the event,
      and the value is the event.

    Returns:
      List of tuples containing failed events indexes and corresponding errors.
    """
    invalid_indices_and_errors = []

    retries = 0
    while retries < _MAX_RETRIES:
      request_body = self._create_request([event[1] for event in batch])
      request = self._cm_service.conversions().batchinsert(
          profileId=self._profile_id, body=request_body)
      response = request.execute()

      response_error_indices, reasons = (
          self._extract_failed_events_info_from_response(response, len(batch)))

      indexes_to_remove_from_batch = []

      indexes_to_remove_from_batch.extend(set(range(len(batch))) -
                                          set(response_error_indices))

      for response_error_index, reason in zip(response_error_indices, reasons):
        if reason not in _RETRIABLE_ERRORS:
          invalid_indices_and_errors.append(
              (batch[response_error_index][0],
               errors.ErrorNameIDMap.NON_RETRIABLE_ERROR_EVENT_NOT_SENT))
          indexes_to_remove_from_batch.append(response_error_index)

      for index_to_remove in sorted(indexes_to_remove_from_batch, reverse=True):
        del batch[index_to_remove]

      if not batch:
        break

      time.sleep(2**retries)
      retries += 1

    for event in batch:
      invalid_indices_and_errors.append(
          (event[0], errors.ErrorNameIDMap.RETRIABLE_ERROR_EVENT_NOT_SENT))

    return invalid_indices_and_errors

  def send_events(self, blb: blob.Blob) -> blob.Blob:
    """Sends events to Campaign Manager API.

    Args:
      blb: A blob containing Customer Match data to send.

    Returns:
      A blob containing updated data about any failing events or reports.
    """
    valid_events, invalid_indices_and_errors = (
        self._validate_and_prepare_events_to_send(blb.events))
    batches = self._batch_generator(valid_events)

    for batch in batches:
      try:
        invalid_events = self._send_batch(batch)
      except errors.DataOutConnectorAuthenticationError as error:
        for event in batch:
          invalid_indices_and_errors.append((event[0], error.error_num))
      else:
        invalid_indices_and_errors.extend(invalid_events)

    for event in invalid_indices_and_errors:
      blb.append_failed_event(event[0] + blb.position, blb.events[event[0]],
                              event[1].value)

    return blb
