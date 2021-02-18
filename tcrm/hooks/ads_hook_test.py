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

"""Tests for gps_building_blocks.tcrm.hooks.ads_hook."""

import unittest
import unittest.mock as mock

from googleads import adwords
from googleads import errors as googleads_errors

from gps_building_blocks.tcrm.hooks import ads_hook
from gps_building_blocks.tcrm.utils import errors


class GoogleAdsHookTest(unittest.TestCase):

  def setUp(self):
    """Setup function for each unit test."""

    super(GoogleAdsHookTest, self).setUp()

    self.yaml_doc = ('adwords:\n'
                     '  client_customer_id: 123-456-7890\n'
                     '  developer_token: abcd\n'
                     '  client_id: test.apps.googleusercontent.com\n'
                     '  client_secret: secret\n'
                     '  refresh_token: 1//token\n')
    self.user_list_name = 'my_user_list'

    self.test_ads_hook = ads_hook.GoogleAdsHook(self.yaml_doc)

    self.mock_service = mock.MagicMock()

    self.patcher = mock.patch.object(adwords, 'AdWordsClient',
                                     spec=True)
    self.mock_ads_client = self.patcher.start()
    self.mock_ads_client.LoadFromString.return_value = self.mock_ads_client
    self.mock_ads_client.GetService.return_value = self.mock_service

  def tearDown(self):
    super(GoogleAdsHookTest, self).tearDown()
    if self.mock_ads_client is adwords.AdWordsClient:
      self.patcher.stop()

  def test_get_user_list_id_incorrect_yaml_file(self):
    self.patcher.stop()
    with self.assertRaises(errors.DataOutConnectorAuthenticationError):
      hook = ads_hook.GoogleAdsHook('')
      hook.get_user_list_id(self.user_list_name)

  def test_get_user_list_id_incorrect_api_verison(self):
    self.patcher.stop()
    with self.assertRaises(errors.DataOutConnectorValueError):
      hook = ads_hook.GoogleAdsHook(self.yaml_doc, 'bad api version')
      hook.get_user_list_id(self.user_list_name)

  def test_get_user_list_id_get_api_raise_expception(self):
    err = googleads_errors.GoogleAdsServerFault(document=None)
    self.mock_service.get.side_effect = err
    with self.assertRaises(errors.DataOutConnectorError):
      self.test_ads_hook.get_user_list_id(self.user_list_name)
    self.mock_ads_client.GetService.assert_called_with(
        ads_hook.ServiceType.ADWORDS_USER_LIST_SERVICE.value, 'v201809')

  def test_get_user_list_id_raises_list_do_not_exist(self):
    self.mock_service.get.return_value = {}
    with self.assertRaises(errors.DataOutConnectorValueError):
      self.test_ads_hook.get_user_list_id(self.user_list_name)

  def test_get_user_list_id_get_existing_list(self):
    self.mock_service.get.return_value = {'entries': [{'id': 12345}]}
    user_list_id = self.test_ads_hook.get_user_list_id(self.user_list_name)
    self.assertEqual(user_list_id, 12345)

  def test_create_user_list_id_raises_data_out_connector_error(self):
    self.mock_service.mutate.side_effect = (googleads_errors.
                                            GoogleAdsServerFault(document=None))
    with self.assertRaises(errors.DataOutConnectorError):
      self.test_ads_hook.create_user_list(self.user_list_name)

  def test_create_user_list_id_bad_response(self):
    self.mock_service.mutate.return_value = {'bad_key': [{'id': 23456}]}
    with self.assertRaises(errors.DataOutConnectorError):
      self.test_ads_hook.create_user_list(self.user_list_name)

  def test_create_user_list_id_successfully_return(self):
    self.mock_service.mutate.return_value = {'value': [{'id': 23456}]}
    new_id = self.test_ads_hook.create_user_list(self.user_list_name)
    self.assertEqual(new_id, 23456)

  def test_create_mobile_advertising_user_list(self):
    self.mock_service.mutate.return_value = {'value': [{'id': 23456}]}

    new_id = self.test_ads_hook.create_user_list(
        self.user_list_name,
        ads_hook.UploadKeyType.MOBILE_ADVERTISING_ID,
        8,
        'fancyappid')

    self.mock_service.mutate.assert_called_with([{
        'operator': 'ADD',
        'operand': {
            'xsi_type': 'CrmBasedUserList',
            'name': 'my_user_list',
            'description': 'A list of users uploaded from Adwords API via TCRM',
            'membershipLifeSpan': 8,
            'uploadKeyType': 'MOBILE_ADVERTISING_ID',
            'appId': 'fancyappid'
        }
    }])
    self.assertEqual(new_id, 23456)

  def test_add_members_to_user_list_raise_error_at_mutate_members(self):
    self.mock_service.mutateMembers.side_effect = (
        googleads_errors.GoogleAdsServerFault(document=None))
    with self.assertRaises(errors.DataOutConnectorAuthenticationError):
      self.test_ads_hook.add_members_to_user_list(34567, [{}])

  def test_add_members_to_user_list_raise_error_when_result_is_failed(self):
    self.mock_service.mutateMembers.return_value = {
        'userLists': [{
            'dataUploadResult': 'FAILURE'
        }]
    }
    with self.assertRaises(errors.DataOutConnectorSendUnsuccessfulError):
      self.test_ads_hook.add_members_to_user_list(34567, [{}])

  def test_add_members_to_user_list_raise_error_at_checking_response(self):
    self.mock_service.mutateMembers.return_value = {}
    with self.assertRaises(errors.DataOutConnectorSendUnsuccessfulError):
      self.test_ads_hook.add_members_to_user_list(34567, [{}])

  def test_add_offline_conversions(self):
    self.mock_service.mutate.return_value = []
    partial_failures = self.test_ads_hook.add_offline_conversions({})
    self.assertFalse(partial_failures)

  def test_add_offline_conversions_failed_with_authentication(self):
    self.mock_service.mutate.side_effect = (
        googleads_errors.GoogleAdsServerFault(document=None))
    with self.assertRaises(errors.DataOutConnectorAuthenticationError):
      self.test_ads_hook.add_offline_conversions({})


if __name__ == '__main__':
  unittest.main()
