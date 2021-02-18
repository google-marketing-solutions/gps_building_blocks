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

"""Custom Hook for sending offline conversions to Google Ads via Adwords API."""

import copy
import enum
import re
import time
from typing import Any, Dict, List, Tuple, Generator

from gps_building_blocks.tcrm.hooks import ads_hook
from gps_building_blocks.tcrm.hooks import output_hook_interface
from gps_building_blocks.tcrm.utils import blob
from gps_building_blocks.tcrm.utils import errors


class RequiredFields(enum.Enum):
  """Manadatory fields for conversions."""

  CONVERSION_NAME = 'conversionName'
  CONVERSION_TIME = 'conversionTime'
  CONVERSION_VALUE = 'conversionValue'
  GOOGLE_CLICK_ID = 'googleClickId'


_CONVERSION_BATCH_MAX_COUNT = 2000
_MAX_RETRIES = 5

_RETRIABLE_ERRORS = ('InternalApiError.UNEXPECTED_INTERNAL_API_ERROR',
                     'InternalApiError.TRANSIENT_ERROR',
                     'DatabaseError.DATABASE_ERROR',
                     'DatabaseError.UNKNOWN')

# Conversion time should follow below pattern for avoiding
# INVALID_STRING_DATE_TIME error, for detail:
# https://developers.google.com/adwords/api/docs/reference/v201809/OfflineConversionFeedService.DateError
# Good date time string example: 20191030 122301 Asia/Calcutta
_RE_STRING_DATE_TIME = r'\d{8} \d{6} [\w\/-]+'


class GoogleAdsOfflineConversionsHook(
    ads_hook.GoogleAdsHook, output_hook_interface.OutputHookInterface):
  """Custom hook to send offline conversions to Google Ads via Adwords API."""

  def __init__(self, ads_credentials: str, **kwargs) -> None:
    """Initialises the class.

    Args:
      ads_credentials: A dict of Adwords client ids and tokens.
        Reference for desired format:
          https://developers.google.com/adwords/api/docs/guides/first-api-call
      **kwargs: Other optional arguments.
    """
    super().__init__(ads_yaml_doc=ads_credentials)

  def _validate_required_fields(self, event: Dict[str, Any]) -> None:
    """Validates all required fields are present in the event JSON.

    Args:
      event: Offline Conversion JSON event.

    Raises:
      AssertionError: If any any violation is found.
    """
    if not all(field.value in event.keys()
               for field in RequiredFields):
      raise errors.DataOutConnectorValueError(
          f'Event is missing at least one mandatory field(s)'
          f' {[field.value for field in RequiredFields]}',
          errors.ErrorNameIDMap.ADS_OC_HOOK_ERROR_MISSING_MANDATORY_FIELDS)

    if not event['conversionName'] or len(event['conversionName']) > 100:
      raise errors.DataOutConnectorValueError(
          'Length of conversionName should be <= 100.',
          errors.ErrorNameIDMap
          .ADS_OC_HOOK_ERROR_INVALID_LENGTH_OF_CONVERSION_NAME)

    if not re.match(_RE_STRING_DATE_TIME, event['conversionTime']):
      raise errors.DataOutConnectorValueError(
          'conversionTime should be formatted: yyyymmdd hhmmss [tz]',
          errors.ErrorNameIDMap
          .ADS_OC_HOOK_ERROR_INVALID_FORMAT_OF_CONVERSION_TIME)

    if event['conversionValue'] < 0:
      raise errors.DataOutConnectorValueError(
          'conversionValue should be greater than or equal to 0.',
          errors.ErrorNameIDMap.ADS_OC_HOOK_ERROR_INVALID_CONVERSION_VALUE)

    if not event['googleClickId'] or len(event['googleClickId']) > 512:
      raise errors.DataOutConnectorValueError(
          'Length of googleClickId should be between 1 and 512.',
          errors.ErrorNameIDMap
          .ADS_OC_HOOK_ERROR_INVALID_LENGTH_OF_GOOGLE_CLICK_ID)

  def _validate_events(
      self, events: List[Dict[str, Any]]
      ) -> Tuple[List[Tuple[int, Dict[str, Any]]],
                 List[Tuple[int, errors.ErrorNameIDMap]]]:
    """Splits input events into lists of valid and invalid events.

    Args:
      events: Offline conversion events to send.

    Returns:
      Tuple containing following two lists:
      List1 : Valid conversion events.
      List2 : Tuples of invalid events indices and their corresponding errors.
    """
    valid_events = []
    invalid_indices_and_errors = []

    for i, event in enumerate(events):
      try:
        self._validate_required_fields(event)
      except errors.DataOutConnectorValueError as error:
        invalid_indices_and_errors.append((i, error.error_num))
      else:
        valid_events.append((i, event))

    return valid_events, invalid_indices_and_errors

  def _batch_generator(
      self,
      events: List[Tuple[int, Dict[str, Any]]]
      ) -> Generator[List[Tuple[int, Dict[str, Any]]], None, None]:
    """Creates a batch generator of _CONVERSION_BATCH_MAX_SIZE batches.

    Args:
      events: Offline conversion events to send.

    Yields:
      A batch list of _CONVERSION_BATCH_MAX_SIZE events
    """
    for i in range(0, len(events), _CONVERSION_BATCH_MAX_COUNT):
      yield events[i:i + _CONVERSION_BATCH_MAX_COUNT]

  def _create_single_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
    """Generates a single OfflineConversionFeedOperation JSON object.

    Args:
      event: Single OfflineConversionFeed object JSON.

    Returns:
      Single OfflineConversionFeedOperation JSON object to send to ads.
    """
    offline_conversion_operation = {
        'operator': 'ADD',
        'operand': copy.deepcopy(event)
    }
    return offline_conversion_operation

  def _extract_failed_events_info_from_response(
      self, partial_failure_errors: List[Dict[str, Any]]
  ) -> Tuple[List[int], List[str]]:
    """Extracts failed event indices and reasons from Ads API response object.

    Args:
      partial_failure_errors: List of failed operations from which error indices
        will be extracted. For more details on partial errors refer to
          https://developers.google.com/adwords/api/docs/guides/partial-failure

    Returns:
      Tuple of error indices and error reasons.
    """
    error_indices = []
    reasons = []
    if partial_failure_errors:
      path_elements = [error['fieldPathElements']
                       for error in partial_failure_errors
                       if error is not None]
      error_indices = [elements[0]['index'] for elements in path_elements]
      reasons = [error['errorString']
                 for error in partial_failure_errors
                 if error is not None]
    return error_indices, reasons

  def _send_batch(self, batch: List[Tuple[int, Dict[str, Any]]]
                 ) -> List[Tuple[int, errors.ErrorNameIDMap]]:
    """Calls the Adwords API to send the batch of offline conversions.

    Retries sending if there is a partial failure wherein some conversions
    are sent and some are not due to INTERNAL error as per the API response.

    Args:
      batch: A list of index and event tuples.

    Returns:
      List of tuples containing failed events indexes and corresponding errors.
    """
    invalid_indices_and_errors = []

    retries = 0
    while retries < _MAX_RETRIES:
      operations = [self._create_single_event(event[1]) for event in batch]
      response = self.add_offline_conversions(operations)
      (response_error_indices,
       reasons) = self._extract_failed_events_info_from_response(
           response['partialFailureErrors'])

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

      if not bool(batch):
        break

      time.sleep(2**retries)
      retries += 1

    for event in batch:
      invalid_indices_and_errors.append(
          (event[0], errors.ErrorNameIDMap.RETRIABLE_ERROR_EVENT_NOT_SENT))

    return invalid_indices_and_errors

  def send_events(self, blb: blob.Blob) -> blob.Blob:
    """Sends all events to Google Ads via Adwords API.

    Args:
      blb: A blob containing Customer Match data to send.

    Returns:
      A blob containing updated data about any failing events or reports.
    """
    valid_events, invalid_indices_and_errors = self._validate_events(blb.events)
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
