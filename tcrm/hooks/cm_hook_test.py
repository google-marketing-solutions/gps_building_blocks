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

"""Unit tests for tcrm.hooks.cm_hook."""

import enum
from typing import Any, Dict, List
import unittest
import unittest.mock as mock

from gps_building_blocks.cloud.utils import cloud_auth
from gps_building_blocks.tcrm.hooks import cm_hook
from gps_building_blocks.tcrm.utils import blob
from gps_building_blocks.tcrm.utils import errors


def _create_response_status(event: Dict[str, Any],
                            error_code: str) -> Dict[Any, Any]:
  """Creates the API response for a given conversion event and error code."""
  status = {
      'kind':
          'dfareporting#conversionStatus',
      'conversion': event,
  }
  if error_code:
    status['errors'] = [{
        'kind': 'dfareporting#conversionError',
        'code': error_code,
        'message': 'Error raised for the event.'
    }]
  else:
    status['errors'] = []
  return status


def _get_response(events: List[Dict[str, Any]],
                  errors_list: List[str],
                  batch_error: str = '',
                  has_failures: str = True) -> Dict[Any, Any]:
  """Creates the API response with various errors statuses."""
  statuses = []

  for event, error in zip(events, errors_list):
    statuses.append(_create_response_status(event, error))

  api_response = {'kind': 'dfareporting#conversionsBatchInsertResponse',
                  'hasFailures': has_failures,
                  'status': statuses}
  if batch_error:
    api_response['error'] = batch_error

  return api_response


def _get_conversion_events(
    count: int, event: Dict[str, Any]) -> List[Dict[str, Any]]:
  """Creates a list of 'count' number of events with unique ordinal Ids.

  Args:
    count: Number of events required in the list.
    event: Payload event to be replicated in the list.

  Returns:
    List containing 'count' number of events.
  """
  events = []
  for i in range(count):
    event_copy = dict(event)
    event_copy['ordinal'] = 'ordinal' + str(i)
    events.append(event_copy)
  return events


class RequiredFields(enum.Enum):
  """Mandatory Fields for Conversion upload."""
  GCLID = 'gclid'
  FLOODLIGHT_ACTIVITY_ID = 'floodlightActivityId'
  FLOODLIGHT_CONFIGURATION_ID = 'floodlightConfigurationId'
  ORDINAL = 'ordinal'
  TIMESTAMPMICROS = 'timestampMicros'
  QUANTITY = 'quantity'
  VALUE = 'value'


class PayloadBuilderTest(unittest.TestCase):

  def setUp(self):
    """Setup function for each unit test."""
    super(PayloadBuilderTest, self).setUp()

    self.event_test_conversion = {
        'kind': 'dfareporting#conversion',
        RequiredFields.
        FLOODLIGHT_CONFIGURATION_ID.value: 'floodlightConfigurationId',
        RequiredFields.FLOODLIGHT_ACTIVITY_ID.value: 'floodlightActivityId',
        RequiredFields.GCLID.value: 'gclid',
        RequiredFields.ORDINAL.value: 'ordinal',
        RequiredFields.TIMESTAMPMICROS.value: 'timestampMicros',
        RequiredFields.QUANTITY.value: 'quantity',
        RequiredFields.VALUE.value: 'value',
        'customVariables': [{
            'kind': 'dfareporting#customFloodlightVariable',
            'type': 'U11',
            'value': 'custom_value',
        }]
    }
    self.payload_builder = cm_hook.PayloadBuilder()

  def test_payload_builder_with_missing_gclid(self):
    """Validate required field GCLID is present in conversion event.

    This test is to validate that an error is raised if the mandatory fields
    "gclid" is missing from the payload.
    """
    self.event_test_conversion.pop(RequiredFields.GCLID.value)
    with self.assertRaises(errors.DataOutConnectorInvalidPayloadError):
      self.payload_builder.generate_single_payload(self.event_test_conversion)

  def test_payload_builder_with_missing_ordinal(self):
    """Validate required field ORDINAL is present in conversion event.

    This test is to validate that an error is raised if the mandatory fields
    "ordinal" is missing from the payload.
    """
    self.event_test_conversion.pop(RequiredFields.ORDINAL.value)
    with self.assertRaises(errors.DataOutConnectorInvalidPayloadError):
      self.payload_builder.generate_single_payload(self.event_test_conversion)

  def test_payload_builder_no_exception_with_all_required_fields(self):
    """Validate that no error is raised if all required fields are present."""
    try:
      self.payload_builder.generate_single_payload(self.event_test_conversion)
    except errors.DataOutConnectorInvalidPayloadError as e:
      self.fail('All mandatory fields present still raised an error.'
                'Validation of required field logic is faulty. {}'.format(e))

  def test_payload_builder_with_invalid_custom_variable_type_field(self):
    """Validate customVariable.type field is of valid format {U1-U99}.

    This test is to validate that an error is raised if the customVariable.type
    field is not of the valid format {U1-U99}.
    """
    self.event_test_conversion['customVariables'][0]['type'] = 'Uxx'
    with self.assertRaises(errors.DataOutConnectorInvalidPayloadError):
      self.payload_builder.generate_single_payload(self.event_test_conversion)

  def test_payload_builder_for_invalid_custom_variable_value_field_length(self):
    """Validate customVariable.value field is of <=50 char length.

    This test is to validate that an error is raised if the customVariable.value
    field is of length greater than 50 chars.
    """
    long_string = '123456789012345678901234567890123456789012345678901234567890'
    self.event_test_conversion['customVariables'][0]['value'] = long_string
    with self.assertRaises(errors.DataOutConnectorInvalidPayloadError):
      self.payload_builder.generate_single_payload(self.event_test_conversion)

  def test_payload_builder_for_valid_custom_variable_value_field_length(self):
    """Validate customVariable.value field is of <=50 char length.

    This test is to validate that no error is raised if the customVariable.value
    field is of length <=50 chars.
    """
    self.event_test_conversion['customVariables'][0]['value'] = '123'
    try:
      self.payload_builder.generate_single_payload(self.event_test_conversion)
    except errors.DataOutConnectorInvalidPayloadError as e:
      self.fail('Valid length of customVariables.value field raised an error.'
                ' Validation of customVariables field is faulty. {}'. format(e))


class CampaignManagerHookTest(unittest.TestCase):

  def setUp(self):
    """Setup function for each unit test."""
    super(CampaignManagerHookTest, self).setUp()
    self.addCleanup(mock.patch.stopall)

    self.build_impersonated_client_mock = mock.patch.object(
        cloud_auth, 'build_impersonated_client', autospec=True)
    self.build_impersonated_client_mock.return_value = mock.Mock()
    self.build_impersonated_client_mock.start()

    self.cm_hook = cm_hook.CampaignManagerHook('dfareporting', '123456')

    self.request_mock = mock.Mock()
    (self.cm_hook._cm_service.conversions.
     return_value.batchinsert.return_value) = self.request_mock

    self.event_test_conversion = {
        'kind': 'dfareporting#conversion',
        RequiredFields.
        FLOODLIGHT_CONFIGURATION_ID.value: 'floodlightConfigurationId',
        RequiredFields.FLOODLIGHT_ACTIVITY_ID.value: 'floodlightActivityId',
        RequiredFields.GCLID.value: 'gclid',
        RequiredFields.ORDINAL.value: 'ordinal',
        RequiredFields.TIMESTAMPMICROS.value: 'timestampMicros',
        RequiredFields.QUANTITY.value: 'quantity',
        RequiredFields.VALUE.value: 'value',
        'customVariable': [{
            'kind': 'dfareporting#customFloodlightVariable',
            'type': 'U11',
            'value': 'custom_value',
        }]
    }
    self.errors_list = ['INTERNAL', '', 'NOT_FOUND',
                        'PERMISSION_DENIED', 'INVALID_ARGUMENT']

  def _execute_and_assert_num_of_failed_event(self, events, num_failed_events):
    """Wraps calling send_event and assertion to avoid duplication."""
    blb = blob.Blob(events=events, location='')

    blb = self.cm_hook.send_events(blb)

    self.assertEqual(len(blb.failed_events), num_failed_events)

  def test_send_events_no_events_in_blob_does_nothing(self):
    """Validate upload_batch call api to upload events."""
    events = {}
    self.request_mock.execute.return_value = {}

    self._execute_and_assert_num_of_failed_event(events=events,
                                                 num_failed_events=0)

    self.request_mock.execute.assert_not_called()

  def test_send_events_call_api_execute(self):
    """Validate upload_batch call api to upload events."""
    errors_list = [''] * 2
    events = _get_conversion_events(2, self.event_test_conversion)
    self.request_mock.execute.return_value = _get_response(
        events=events, errors_list=errors_list)

    self._execute_and_assert_num_of_failed_event(events, 0)
    self.request_mock.execute.assert_called_once()

  def test_send_events_all_bad_events(self):
    """Validate upload_batch call api to upload events."""
    errors_list = ['INVALID_ARGUMENT'] * 2
    events = _get_conversion_events(2, self.event_test_conversion)
    self.request_mock.execute.return_value = _get_response(
        events=events, errors_list=errors_list)

    self._execute_and_assert_num_of_failed_event(events, 2)

  def test_send_events_partial_bad_events(self):
    """Validate upload_batch call api to upload events."""
    errors_list = ['']
    events = _get_conversion_events(1, self.event_test_conversion)
    self.request_mock.execute.return_value = _get_response(
        events=events, errors_list=errors_list)
    bad_event = {}
    events.append(bad_event)

    self._execute_and_assert_num_of_failed_event(events, 1)

  def test_send_events_sends_batches_successfully(self):
    """Validate send_events sends events in batches."""
    more_than_2_batches = (cm_hook._CONVERSION_BATCH_MAX_SIZE * 2) + 5
    errors_list = [''] * more_than_2_batches
    events = _get_conversion_events(more_than_2_batches,
                                    self.event_test_conversion)
    self.request_mock.execute.return_value = _get_response(
        events=events, errors_list=errors_list)

    self._execute_and_assert_num_of_failed_event(events, 0)

if __name__ == '__main__':
  unittest.main()
