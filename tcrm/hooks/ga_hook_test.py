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

"""Tests for tcrm.hooks.ga_hook."""

import re
import unittest
import unittest.mock as mock
import parameterized

from gps_building_blocks.tcrm.hooks import ga_hook
from gps_building_blocks.tcrm.utils import blob
from gps_building_blocks.tcrm.utils import errors
from gps_building_blocks.tcrm.utils import retry_utils


class PayloadBuilderTest(unittest.TestCase):

  def setUp(self):
    """Setup function for each unit test."""
    super(PayloadBuilderTest, self).setUp()

    self.test_tracking_id = 'UA-12323-4'
    self.payload_builder = ga_hook.PayloadBuilder(self.test_tracking_id)
    self.event_test_data = {
        'ec': 'ClientID',
        'ea': 'test_event_action',
        'el': '20190423',
        'ev': 1,
        'cid': '12345.456789'
    }
    self.event_payload_list = self.generate_expected_payload_str_list(
        self.event_test_data)
    self.event_payload_list.append('tid={t}'.format(t=self.test_tracking_id))

  def generate_expected_payload_str_list(self, payload_dict):
    """Generate expected payload str list.

    Generate payload key value pair string test data for result verification.

    Args:
      payload_dict: paylod dict object, contain params and corresponding value.

    Returns:
      payload_str_list: list that contain key=value pair string.
    """
    return ['{k}={v}'.format(k=k, v=self.event_test_data[k])
            for k in payload_dict]

  def test_payload_builder_send_single_hit_with_valid_tracking_id(self):
    """Test payload builder with valid tracking id."""
    self.assertEqual(self.payload_builder.tracking_id, self.test_tracking_id)

  def test_payload_builder_gen_single_payload_with_valid_hit_type(self):
    """Test payload builder on generating single payload with valid hit type."""
    actual_payload_str = self.payload_builder.generate_single_payload(
        ga_hook.HitTypes.EVENT, self.event_test_data)

    result = all([re.search(t, actual_payload_str)
                  for t in self.event_payload_list])
    self.assertTrue(result)

  def test_payload_builder_gen_single_payload_with_missing_cid_and_uid(self):
    """Test payload builder on generating single payload without cid nor uid."""
    self.event_test_data.pop('cid')
    with self.assertRaises(errors.DataOutConnectorValueError):
      self.payload_builder.generate_single_payload(ga_hook.HitTypes.EVENT,
                                                   self.event_test_data)

  def test_payload_builder_gen_single_payload_with_valid_cid(self):
    """Test payload builder on generating single payload with valid cid."""
    actual_payload_str = self.payload_builder.generate_single_payload(
        ga_hook.HitTypes.EVENT, self.event_test_data)

    result = all([re.search(t, actual_payload_str)
                  for t in self.event_payload_list])
    self.assertTrue(result)

  def test_payload_builder_gen_single_payload_with_valid_uid(self):
    """Test payload builder on generating single payload with valid uid."""

    # remove the default cid test data and add uid test data
    self.event_test_data.pop('cid')
    self.event_test_data['uid'] = '123456789.1236546'

    actual_payload_str = self.payload_builder.generate_single_payload(
        ga_hook.HitTypes.EVENT, self.event_test_data)

    expected_payload_list = self.generate_expected_payload_str_list(
        self.event_test_data)
    validation_result = all([re.search(t, actual_payload_str)
                             for t in expected_payload_list])
    self.assertTrue(validation_result)

  def test_payload_builder_gen_single_payload_with_exceed_max_payload_size(
      self):
    """Tests builder generating single payload with over 8192 bytes' payload."""
    dummy_payload = {'dp': 'x' * 8192, 'cid': '12345.456789'}

    with self.assertRaisesRegex(
        errors.DataOutConnectorInvalidPayloadError,
        'Error 81 - DataOutConnectorInvalidPayloadError: '
        'Hit size 8312 exceeds limitation 8192'):
      self.payload_builder.generate_single_payload(
          ga_hook.HitTypes.SCREENVIEW,
          dummy_payload)

  def test_payload_builder_gen_batch_payload_with_over_20_hits_included(self):
    """Test builder generating batch payload with over 20 hits included."""
    dummy_payloads = [self.event_test_data] * 21

    with self.assertRaises(errors.DataOutConnectorInvalidPayloadError):
      self.payload_builder.generate_batch_payload(ga_hook.HitTypes.EVENT,
                                                  dummy_payloads)

  def test_payload_builder_gen_batch_payload_exceeds_max_single_payload_size(
      self):
    """Tests builder generating batch payload with over 8192 bytes."""
    dummy_payload = {'dp': 'x' * 8193, 'cid': '12345.456789'}

    with self.assertRaisesRegex(
        errors.DataOutConnectorInvalidPayloadError,
        'Error 81 - DataOutConnectorInvalidPayloadError: '
        'Hit size 8313 exceeds limitation 8192'):
      self.payload_builder.generate_batch_payload(ga_hook.HitTypes.SCREENVIEW,
                                                  [dummy_payload])

  def test_payload_builder_gen_batch_payload_with_exceed_max_batch_payload_size(
      self):
    """Tests builder on generating batch payload with over 16384 bytes."""
    dummy_payloads = [{'dp': 'x' * 8071, 'cid': '12345.456789'}] * 19

    with self.assertRaisesRegex(
        errors.DataOutConnectorInvalidPayloadError,
        'Error 81 - DataOutConnectorInvalidPayloadError: '
        'Hit size 154765 exceeds limitation 16384'):
      self.payload_builder.generate_batch_payload(ga_hook.HitTypes.SCREENVIEW,
                                                  dummy_payloads)

  def test_payload_builder_gen_batch_payload_with_valid_payloads(self):
    """Test payload builder on generating single payload with cid."""
    vr = True
    dp_str = 'x' * 1000
    dummy_payloads = [{'dp': dp_str, 'cid': '12345.456789'}] * 10
    expected_payload_list = ['cid=12345.456789', 'dp={d}'.format(d=dp_str)]

    actual_payload_list = self.payload_builder.generate_batch_payload(
        ga_hook.HitTypes.SCREENVIEW, dummy_payloads).split('\n')

    for aps in actual_payload_list:
      vr = vr and all([re.search(t, aps) for t in expected_payload_list])
    self.assertTrue(vr)
    self.assertEqual(len(actual_payload_list), 10)


class GoogleAnalyticsHookTest(unittest.TestCase):

  def setUp(self):
    """Setup function for each unit test."""

    super(GoogleAnalyticsHookTest, self).setUp()

    self.test_tracking_id = 'UA-12323-4'
    self.payload_builder = ga_hook.PayloadBuilder(self.test_tracking_id)

    self.event_test_data = {
        'ec': 'ClientID',
        'ea': 'test_event_action',
        'el': '20190423',
        'ev': 1,
        'cid': '12345.456789'
    }
    self.small_event = {
        'cid': '12345.67890',
        'ec': 'ClientID',
        'ea': 'test_event_action',
        'el': '20190423',
        'ev': 1,
        'z': '1558517072202080'
    }
    # Both of the below are approx 4K of data
    self.medium_event = {**self.small_event, 'ea': 'x' * 3800}
    self.utf8_event = {**self.small_event, 'ea': b'\xf0\xa9\xb8\xbd' * 320}

    self.test_hook = ga_hook.GoogleAnalyticsHook(self.test_tracking_id,
                                                 self.event_test_data)

    self.test_hook._send_http_request = mock.MagicMock(autospec=True)
    self.test_hook._send_http_request.return_value = mock.Mock(ok=True)
    self.test_hook._send_http_request.return_value.status = 200

  def test_ga_hook_get_invalid_tracking_id(self):
    """Test GoogleAnalyticsHook with invalid tracking id."""
    with self.assertRaises(errors.DataOutConnectorValueError):
      ga_hook.GoogleAnalyticsHook('UA-123-b', self.event_test_data)

  def test_ga_hook_send_single_hit_with_dry_run(self):
    """Test GoogleAnalyticsHook sends single hit with dryrun."""
    self.test_hook.dry_run = True
    self.test_hook.send_hit('', '')
    self.test_hook.dry_run = False

    self.test_hook._send_http_request.assert_not_called()

  def test_ga_hook_send_single_hit_without_dry_run(self):
    """Test GoogleAnalyticsHook sends single hit with dryrun."""
    self.test_hook.send_hit('', '')

    self.test_hook._send_http_request.assert_called_once()

  def test_ga_hook_send_http_request_with_single_hit_setup(self):
    """Test GoogleAnalyticsHook sends request with single hit setup."""
    test_payload = self.payload_builder.generate_single_payload(
        ga_hook.HitTypes.EVENT, self.event_test_data)
    hit_url = self.test_hook._get_hit_url(ga_hook.SendTypes.SINGLE.value)

    self.test_hook.send_hit(test_payload)

    self.test_hook._send_http_request.assert_called_once_with(
        test_payload, hit_url, {})

  def test_ga_hook_send_http_request_with_user_agent(self):
    """Test GoogleAnalyticsHook sends request with user agent."""
    test_header = {'User-Agent': 'Test Agent'}
    test_payload = self.payload_builder.generate_single_payload(
        ga_hook.HitTypes.EVENT, self.event_test_data)
    hit_url = self.test_hook._get_hit_url(ga_hook.SendTypes.SINGLE.value)

    self.test_hook.send_hit(
        test_payload, user_agent=test_header['User-Agent'])

    self.test_hook._send_http_request.assert_called_once_with(
        test_payload, hit_url, test_header)

  def test_ga_hook_send_http_request_with_batch_hit_setup(self):
    """Test GoogleAnalyticsHook sends request with batch hit setup."""
    test_payload = self.payload_builder.generate_batch_payload(
        ga_hook.HitTypes.EVENT, [self.event_test_data])

    self.test_hook.send_hit(test_payload, send_type=ga_hook.SendTypes.BATCH)

    hit_url = self.test_hook._get_hit_url('batch')
    self.test_hook._send_http_request.assert_called_once_with(
        test_payload, hit_url, {})

  def test_ga_hook_send_single_hit(self):
    """Test GoogleAnalyticsHook sends single hit."""
    test_hook = ga_hook.GoogleAnalyticsHook(self.test_tracking_id,
                                            self.event_test_data,
                                            False)
    test_payload = self.payload_builder.generate_single_payload(
        ga_hook.HitTypes.EVENT, self.event_test_data)

    with mock.patch('urllib.request.urlopen') as urlopen_mock:
      mock_response = urlopen_mock.return_value.__enter__.return_value
      mock_response.status = 200
      test_hook.send_hit(test_payload)

      urlopen_mock.assert_called_once()

  @parameterized.parameterized.expand([[400], [403], [404], [500]])
  def test_ga_hook_send_batch_hit_with_return_error_status_code(self, r_code):
    """Test GoogleAnalyticsHook sends batch hit with error response."""
    test_hook = ga_hook.GoogleAnalyticsHook(self.test_tracking_id,
                                            self.event_test_data,
                                            False)
    test_payload = self.payload_builder.generate_batch_payload(
        ga_hook.HitTypes.EVENT, [self.event_test_data])

    with mock.patch('urllib.request.urlopen') as urlopen_mock:
      mock_response = urlopen_mock.return_value.__enter__.return_value
      mock_response.status = r_code

      with self.assertRaises(errors.DataOutConnectorSendUnsuccessfulError):
        test_hook.send_hit(test_payload, send_type=ga_hook.SendTypes.BATCH)

  def test_ga_hook_send_batch_hit_with_retrys_on_retriable_error(self):
    """Test GoogleAnalyticsHook retries when retriable error occures."""
    test_hook = ga_hook.GoogleAnalyticsHook(self.test_tracking_id,
                                            self.event_test_data,
                                            False)
    test_payload = self.payload_builder.generate_batch_payload(
        ga_hook.HitTypes.EVENT, [self.event_test_data])

    with mock.patch('urllib.request.urlopen') as urlopen_mock:
      urlopen_mock.return_value.__enter__.return_value.status = 429

      try:
        test_hook.send_hit(test_payload)
      except errors.DataOutConnectorSendUnsuccessfulError:
        pass

      self.assertEqual(urlopen_mock.call_count,
                       retry_utils._RETRY_UTILS_MAX_RETRIES)

  def test_ga_hook_send_events_small_event_batch_contents(self):
    with mock.patch.object(self.test_hook, 'send_hit') as patched_send_hook:
      events = list(self.small_event for x in range(20))
      blb = blob.Blob(events=events, location='')

      self.test_hook.send_events(blb)

      expected_str = ('tid=UA-12323-4&v=1&t=event&z=1558517072202080&'
                      'ec=ClientID&ea=test_event_action&el=20190423&ev=1&'
                      'cid=12345.67890')
      expected_payload = '\n'.join([expected_str] * 20)

      patched_send_hook.assert_called_once_with(
          expected_payload, send_type=ga_hook.SendTypes.BATCH)

  def test_ga_hook_send_events_small_event_batching(self):
    with mock.patch.object(self.test_hook, 'send_hit') as patched_send_hook:
      events = list(self.small_event for x in range(40))
      blb = blob.Blob(events=events, location='')

      self.test_hook.send_events(blb)

      # At <1K each, we can fit 20 in each batch
      self.assertEqual(patched_send_hook.call_count, 2)

  def test_ga_hook_send_events_medium_event_batching(self):
    with mock.patch.object(self.test_hook, 'send_hit') as patched_send_hook:
      events = list(self.medium_event for x in range(20))
      blb = blob.Blob(events=events, location='')

      self.test_hook.send_events(blb)

      # at 4K each, we can fit 4 in each batch
      self.assertEqual(patched_send_hook.call_count, 5)

  def test_ga_hook_send_events_utf8_event_batching(self):
    with mock.patch.object(self.test_hook, 'send_hit') as patched_send_hook:
      events = list(self.utf8_event for x in range(20))
      blb = blob.Blob(events=events, location='')

      self.test_hook.send_events(blb)

      # at 4K each, we can fit 4 in each batch
      self.assertEqual(patched_send_hook.call_count, 5)

  def test_ga_hook_send_events_returns_expected(self):
    """Test GoogleAnalyticsHook returns successful and not event indixes."""
    events = list(self.small_event for x in range(4))
    bad_event = {'cid': '12345.67890',
                 'ec': 'ClientID',
                 'ea': 'test_event_action',
                 'el': '20190423',
                 'ev': 1,
                 'z': '1558517072202080' * 10000}
    events.extend([bad_event] * 2)
    expected = [(4, bad_event, 81), (5, bad_event, 81)]
    blb = blob.Blob(events=events, location='')

    with mock.patch.object(self.test_hook, 'send_hit'):
      blb = self.test_hook.send_events(blb)

      # 4 successful events and 2 unsuccessful event
      self.assertListEqual(blb.failed_events, expected)


if __name__ == '__main__':
  unittest.main()
