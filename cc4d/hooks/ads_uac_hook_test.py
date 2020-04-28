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

"""Tests for google3.third_party.gps_building_blocks.cc4d.hooks.ads_uac_hook."""

import time
import unittest
import unittest.mock as mock
from airflow.hooks import base_hook
import parameterized
import requests
import requests_mock

from gps_building_blocks.cc4d.hooks import ads_uac_hook

_URL_TEMPLATE = ('mock://www.googleadservices.com'
                 '/pagead/conversion/app/1.0'
                 '?dev_token={dev_token}'
                 '&link_id={link_id}'
                 '&app_event_type={app_event_type}'
                 '&rdid={rdid}'
                 '&id_type={id_type}'
                 '&lat={lat}'
                 '&app_version={app_version}'
                 '&os_version={os_version}'
                 '&sdk_version={sdk_version}'
                 '&timestamp={timestamp}')


class AdsUACHookTest(unittest.TestCase):

  def setUp(self):
    """Setup function for each unit test."""
    super(AdsUACHookTest, self).setUp()

    self.adapter = requests_mock.Adapter()
    self.session = requests.Session()
    self.session.mount('mock', self.adapter)

    base_hook.BaseHook.get_connection = mock.MagicMock(autospec=True)
    conn = mock.Mock(password='test_password')
    base_hook.BaseHook.get_connection.return_value = conn

    self.test_hook = ads_uac_hook.AdsUACHook('uac', True)
    self.test_hook.base_url = 'mock://www.googleadservices.com'
    self.test_hook.get_conn = mock.MagicMock(autospec=True)
    self.test_hook.get_conn.return_value = self.session

    self.sample_data = dict(
        link_id='TESTLINKIDTESTLINKID',
        app_event_type='in_app_purchase',
        rdid='843c45cc-e237-4f50-b6aa-843c45cc63d6',
        id_type='advertisingid',
        lat=0,
        app_version='1.2.4',
        os_version='5.0.0',
        sdk_version='1.9.5r6',
        timestamp=time.time(),
        )

    self.sample_url = _URL_TEMPLATE.format(
        dev_token=self.test_hook._get_developer_token(), **self.sample_data)

  def test_send_conversions_to_uac_connection_unavailable(self):
    """Test connection is unable to acquire."""
    base_hook.BaseHook.get_connection.return_value = None

    result = self.test_hook.send_conversions_to_uac(self.sample_data)

    self.assertEqual(400, result.get('status_code'))

  def test_send_conversions_to_uac_missing_dev_token(self):
    """Test missing dev token."""
    conn = mock.Mock(password=None)
    base_hook.BaseHook.get_connection.return_value = conn

    result = self.test_hook.send_conversions_to_uac(self.sample_data)

    self.assertEqual(400, result.get('status_code'))

  @parameterized.parameterized.expand(
      [('link_id'),
       ('app_event_type'),
       ('rdid'),
       ('id_type'),
       ('lat'),
       ('app_version'),
       ('os_version'),
       ('sdk_version'),
       ('timestamp')])
  def test_send_conversions_to_uac_with_missing_field(self, key):
    """Test app conversion payload with missing field."""
    del self.sample_data[key]

    result = self.test_hook.send_conversions_to_uac(self.sample_data)

    self.assertEqual(400, result.get('status_code'))

  @parameterized.parameterized.expand(
      [('app_event_type', 'wrong_type'),
       ('rdid', '843c45cc-e237-5f50-b6aa-843c45cc63d6'),
       ('rdid', '843c45cc-e237-4f50-b6aa-843c4c63d6'),
       ('rdid', '843c45cc-e237-4f50-b6aa-843cSDFSD4c63d6'),
       ('rdid', '843c45cc-e237-4f50-b6aa-843c45cc#3d6'),
       ('id_type', 'wrong_id_type'),
       ('lat', 2)])
  def test_send_conversions_to_uac_with_wrong_field(self, key, value):
    """Test app conversion payload with wrong field."""
    self.sample_data[key] = value

    result = self.test_hook.send_conversions_to_uac(self.sample_data)

    self.assertEqual(400, result.get('status_code'))

  def test_vsend_conversions_to_uac_with_inconsistent_app_event_type(self):
    """Test app conversion payload with inconsistent app event type."""
    self.sample_data['app_event_name'] = 'level_achieved'

    result = self.test_hook.send_conversions_to_uac(self.sample_data)

    self.assertEqual(400, result.get('status_code'))

  def test_send_conversions_to_uac_with_custom_app_event_name(self):
    """Test app conversion payload with custom app event name."""
    self.sample_data['app_event_type'] = 'custom'
    self.sample_data['app_event_name'] = 'level_achieved'
    app_event_data = {'level': 5}
    self.sample_data['app_event_data'] = app_event_data

    result = self.test_hook.send_conversions_to_uac(self.sample_data)

    self.assertEqual(True, result.get('dry_run'))

  def test_send_conversions_to_uac_with_upper_case_rdid(self):
    """Test app conversion payload with upper case rdid."""
    self.sample_data['rdid'] = '843C45cc-e237-4f50-B6aa-843C45cc63d6'

    result = self.test_hook.send_conversions_to_uac(self.sample_data)

    self.assertEqual(True, result.get('dry_run'))

  def test_send_conversions_to_uac_valid_request_and_get_ok(self):
    """Test sending valid request and get expected response."""
    self.adapter.register_uri(
        'POST', self.sample_url, complete_qs=True, json={'attributed': True})
    self.test_hook.dry_run = False
    result = self.test_hook.send_conversions_to_uac(self.sample_data)

    self.assertDictEqual(self.sample_data, result.get('request'))
    self.assertEqual(True, result.get('response').get('attributed'))

  def test_send_conversions_to_uacvalid_request_with_body_and_get_ok(self):
    """Test sending valid request and get expected response."""
    self.sample_data['app_event_type'] = 'custom'
    self.sample_data['app_event_name'] = 'level_achieved'
    app_event_data = {'level': 5}
    self.sample_data['app_event_data'] = app_event_data
    url_template = ('{}&app_event_name={{app_event_name}}'
                    .format(_URL_TEMPLATE))

    self.sample_url = url_template.format(
        dev_token=self.test_hook._get_developer_token(), **self.sample_data)

    self.adapter.register_uri(
        'POST', self.sample_url, complete_qs=True, json={'attributed': True})
    self.test_hook.dry_run = False
    result = self.test_hook.send_conversions_to_uac(self.sample_data)

    self.assertDictEqual(self.sample_data, result.get('request'))
    self.assertEqual(True, result.get('response').get('attributed'))

  def test_send_conversions_to_uac_valid_request_and_get_error(self):
    """Test sending valid request and get http error."""
    self.adapter.register_uri(
        'POST', self.sample_url, complete_qs=True,
        status_code=400, reason='error')
    self.test_hook.dry_run = False

    result = self.test_hook.send_conversions_to_uac(self.sample_data)

    self.assertEqual(400, result.get('status_code'))
    self.assertEqual('error', result.get('error_msg'))

  def test_send_events_with_expected_output(self):
    sample_data_list = [self.sample_data] * 5
    self.adapter.register_uri(
        'POST', self.sample_url, complete_qs=True, json={'attributed': True})
    self.test_hook.dry_run = False

    success, fail, results = self.test_hook.send_events(sample_data_list)

    self.assertCountEqual([0, 1, 2, 3, 4], success)
    self.assertListEqual([], fail)
    self.assertEqual(5, len(results))

  def test_send_events_with_failed_output(self):
    sample_data_list = [self.sample_data] * 5
    self.adapter.register_uri(
        'POST', self.sample_url, complete_qs=True,
        status_code=400, reason='Bad Request')
    self.test_hook.dry_run = False

    success, fail, results = self.test_hook.send_events(sample_data_list)

    self.assertCountEqual([0, 1, 2, 3, 4], fail)
    self.assertListEqual([], success)
    self.assertEqual(5, len(results))

if __name__ == '__main__':
  unittest.main()
