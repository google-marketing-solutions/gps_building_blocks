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

"""Tests for gps_building_blocks.tcrm.hooks.ads_oc_hook."""

import copy
import unittest
from unittest import mock

from google.auth.exceptions import RefreshError
from googleads import adwords

from gps_building_blocks.tcrm.hooks import ads_oc_hook
from gps_building_blocks.tcrm.utils import blob


_RETRIABLE_ERR = 'RETRIABLE'
_NON_RETRIABLE_ERR = 'NON_RETRIABLE'
_SUCCESS = 'SUCCESS'


_event_test_conversion = {
    ads_oc_hook.RequiredFields.CONVERSION_NAME.value: 'conversionName',
    ads_oc_hook.RequiredFields.CONVERSION_TIME.value:
        '20191030 122301 Asia/Calcutta',
    ads_oc_hook.RequiredFields.CONVERSION_VALUE.value: 0.4732,
    ads_oc_hook.RequiredFields.GOOGLE_CLICK_ID.value: '123abc'
}


_input_operation = {
    'operator': 'ADD',
    'operand': _event_test_conversion
}


def _create_api_response(err_types):
  """Creates mock API response.

  err_types explain:
    [_RETRIABLE_ERR, _NON_RETRIABLE_ERR, _SUCCESS] creates a response for 3
      operations processing result.
    1st operation fails with a retriable error.
    2nd operation fails with a non-retriable error.
    3nd operation succeed.

    [_SUCCESS] * 1000 creates a response for 1000 operations processing
    result.
    1000 operations succeed.

  Args:
    err_types: list directs how to create response.
  Returns:
    A dict represents the API response.
  """
  def create_base_error(err, index=0):
    error_base = {
        'fieldPath':
            f'operations[{index}].operand',
        'fieldPathElements': [{
            'field': 'operations',
            'index': index
        }, {
            'field': 'operand',
            'index': None
        }]
    }
    error_base.update(err)
    return error_base

  retriable = {
      'trigger': None,
      'errorString': 'InternalApiError.TRANSIENT_ERROR',
      'ApiError.Type': 'InternalApiError',
      'reason': 'UNKNOWN'
  }
  non_retriable = {
      'trigger': None,
      'errorString': 'OfflineConversionError.CONVERSION_PRECEDES_CLICK',
      'ApiError.Type': 'OfflineConversionError',
      'reason': 'CONVERSION_PRECEDES_CLICK'
  }

  operations = []
  partial_failure_errors = []
  for i, et in enumerate(err_types):
    if et == _RETRIABLE_ERR:
      partial_failure_errors.append(create_base_error(retriable, i))
      operations.append(None)
    elif et == _NON_RETRIABLE_ERR:
      partial_failure_errors.append(create_base_error(non_retriable, i))
      operations.append(None)
    elif et == _SUCCESS:
      operations.append(copy.deepcopy(_event_test_conversion))
  response = {
      'ListReturnValue.Type': 'OfflineConversionFeedReturnValue',
      'value': operations,
      'partialFailureErrors': partial_failure_errors
  }
  return response


class GoogleAdsOfflineConversionsHookTest(unittest.TestCase):

  def setUp(self):
    """Setup function for each unit test."""
    super(GoogleAdsOfflineConversionsHookTest, self).setUp()

    self.googleads_credentials = (
        'adwords:\n'
        '  client_customer_id: 123-456-7890\n'
        '  developer_token: abcd\n'
        '  client_id: test.apps.googleusercontent.com\n'
        '  client_secret: secret\n'
        '  refresh_token: 1//token\n')
    self.test_hook = ads_oc_hook.GoogleAdsOfflineConversionsHook(
        self.googleads_credentials)

    self.addCleanup(mock.patch.stopall)

    self.mock_service = mock.MagicMock()
    self.mock_adwords_client = mock.patch.object(
        adwords, 'AdWordsClient', autospec=True).start()
    (self.mock_adwords_client.
     LoadFromString.return_value) = self.mock_adwords_client
    self.mock_adwords_client.GetService.return_value = self.mock_service

  def _execute_and_assert_one_failed_event(self, events):
    """Wraps calling send_event and assertion to avoid duplication."""
    response = _create_api_response([_SUCCESS])
    self.mock_service.mutate.return_value = response
    blb = blob.Blob(events=events, location='')

    blb = self.test_hook.send_events(blb)

    self.assertEqual(len(blb.failed_events), 1)

  def test_event_has_missing_field(self):
    """Tests bad raw data is in the payload."""
    bad_event = copy.deepcopy(_event_test_conversion)
    del bad_event[ads_oc_hook.RequiredFields.CONVERSION_NAME.value]
    events = [copy.deepcopy(_event_test_conversion), bad_event]

    self._execute_and_assert_one_failed_event(events)

  def test_event_has_very_long_conversion_name(self):
    """Test raw data contains very long google click ID."""
    bad_event = copy.deepcopy(_event_test_conversion)
    bad_event['conversionName'] = 'a' * 101
    events = [copy.deepcopy(_event_test_conversion), bad_event]

    self._execute_and_assert_one_failed_event(events)

  def test_event_has_empty_conversion_name(self):
    """Test raw data contains empty google click ID."""
    bad_event = copy.deepcopy(_event_test_conversion)
    bad_event['conversionName'] = ''
    events = [copy.deepcopy(_event_test_conversion), bad_event]

    self._execute_and_assert_one_failed_event(events)

  def test_event_has_bad_conversion_time(self):
    """Test raw data contains bad conversion time."""
    bad_event = copy.deepcopy(_event_test_conversion)
    bad_event['conversionTime'] = 'bad conversion time'
    events = [copy.deepcopy(_event_test_conversion), bad_event]

    self._execute_and_assert_one_failed_event(events)

  def test_event_has_negative_conversion_value(self):
    """Test raw data contains bad conversion value."""
    bad_event = copy.deepcopy(_event_test_conversion)
    bad_event['conversionValue'] = -1.23
    events = [copy.deepcopy(_event_test_conversion), bad_event]

    self._execute_and_assert_one_failed_event(events)

  def test_event_has_very_long_google_click_id(self):
    """Test raw data contains very long google click ID."""
    bad_event = copy.deepcopy(_event_test_conversion)
    bad_event['googleClickId'] = 'a' * 513
    events = [copy.deepcopy(_event_test_conversion), bad_event]

    self._execute_and_assert_one_failed_event(events)

  def test_event_has_empty_google_click_id(self):
    """Test raw data contains empty google click ID."""
    bad_event = copy.deepcopy(_event_test_conversion)
    bad_event['googleClickId'] = ''
    events = [copy.deepcopy(_event_test_conversion), bad_event]

    self._execute_and_assert_one_failed_event(events)

  def test_retriable_error_occurs(self):
    """Test retriable error occurs and retry is triggered."""
    events = [copy.deepcopy(_event_test_conversion)] * 2

    first_response = _create_api_response([_RETRIABLE_ERR, _SUCCESS])
    second_response = _create_api_response([_SUCCESS])
    self.mock_service.mutate.side_effect = [first_response, second_response]
    blb = blob.Blob(events=events, location='')

    blb = self.test_hook.send_events(blb)

    self.assertEqual(self.mock_service.mutate.call_count, 2)
    self.assertEqual(len(blb.failed_events), 0)

  def test_retraible_error_occurs_and_retry_failed(self):
    """Test retriable error occurs and retry is failed after 5 times."""
    events = [copy.deepcopy(_event_test_conversion)] * 2

    first_response = _create_api_response([_RETRIABLE_ERR, _SUCCESS])
    rest_response = _create_api_response([_RETRIABLE_ERR])
    self.mock_service.mutate.side_effect = [first_response, rest_response,
                                            rest_response, rest_response,
                                            rest_response]
    blb = blob.Blob(events=events, location='')

    blb = self.test_hook.send_events(blb)

    self.assertEqual(self.mock_service.mutate.call_count, 5)
    self.assertEqual(len(blb.failed_events), 1)

  def test_non_retriable_error_occurs(self):
    """Test non retriable error occurs and retry is not triggered."""
    events = [copy.deepcopy(_event_test_conversion)] * 2

    response = _create_api_response([_NON_RETRIABLE_ERR, _SUCCESS])
    self.mock_service.mutate.return_value = response
    blb = blob.Blob(events=events, location='')

    blb = self.test_hook.send_events(blb)

    self.assertEqual(self.mock_service.mutate.call_count, 1)
    self.assertEqual(len(blb.failed_events), 1)

  def test_mixed_retriable_and_non_retriable_error_occurs(self):
    events = [copy.deepcopy(_event_test_conversion)] * 3

    resp1 = _create_api_response([_RETRIABLE_ERR,
                                  _NON_RETRIABLE_ERR,
                                  _SUCCESS])
    resp2 = _create_api_response([_SUCCESS])
    self.mock_service.mutate.side_effect = [resp1, resp2]
    blb = blob.Blob(events=events, location='')

    blb = self.test_hook.send_events(blb)

    self.assertEqual(self.mock_service.mutate.call_count, 2)
    self.assertEqual(len(blb.failed_events), 1)

  def test_upload_failed_due_to_authentication_errors(self):
    """Test Authenticating related error occurs."""
    events = [copy.deepcopy(_event_test_conversion)]
    self.mock_service.mutate.side_effect = RefreshError(
        'invalid_client: Unauthorized')
    blb = blob.Blob(events=events, location='')

    blb = self.test_hook.send_events(blb)

    self.assertEqual(len(blb.failed_events), 1)


if __name__ == '__main__':
  unittest.main()
