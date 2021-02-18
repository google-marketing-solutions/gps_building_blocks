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

"""Tests for gps_building_blocks.tcrm.hooks.ads_cm_hook."""
import hashlib
from typing import Text
import unittest
import unittest.mock as mock

from gps_building_blocks.tcrm.hooks import ads_cm_hook
from gps_building_blocks.tcrm.utils import blob
from gps_building_blocks.tcrm.utils import errors


class GoogleAdsCustomerMatchHookTest(unittest.TestCase):

  def setUp(self):
    """Setup function for each unit test."""
    super(GoogleAdsCustomerMatchHookTest, self).setUp()

    self.googleads_credentials = (
        'adwords:\n'
        '  client_customer_id: 123-456-7890\n'
        '  developer_token: abcd\n'
        '  client_id: test.apps.googleusercontent.com\n'
        '  client_secret: secret\n'
        '  refresh_token: 1//token\n')

    self.contact_info_event_email = {
        'hashedEmail': hashlib.sha256(b'test@test.com').hexdigest(),
    }

    self.contact_info_event_phone_number = {
        'hashedPhoneNumber': hashlib.sha256(b'+81312345678').hexdigest(),
    }

    self.address_info = {
        'hashedFirstName': hashlib.sha256(b'Some').hexdigest(),
        'hashedLastName': hashlib.sha256(b'Body').hexdigest(),
        'countryCode': 'JP',
        'zipCode': '1001234'
    }

    self.mobile_id_event = {
        'mobileId': 'cdda802e-fb9c-47ad-9866-0794d394c912'
    }

    self.crm_id_event = {
        'userId': 'client_predefined_user_id_pattern'
    }

  def create_ads_cm_hook(self,
                         user_list_name: Text = 'test_list_name',
                         upload_key_type: Text = 'CONTACT_INFO',
                         create_list: bool = False,
                         membership_lifespan: int = 8, app_id: Text = None
                         ) -> ads_cm_hook.GoogleAdsCustomerMatchHook:
    """A helper function to create the hook with specified paramaters.

    Args:
      user_list_name: The name of the user list to add members to.
      upload_key_type: The upload key type. Refer to ads_hook.UploadKeyType for
        more information.
      create_list: A flag to enable a new list creation if a list called
        user_list_name doesn't exist.
      membership_lifespan: Number of days a user's cookie stays. Refer to
        ads_hook.GoogleAdsHook for details.
      app_id: An ID required for creating a new list if upload_key_type is
        MOBILE_ADVERTISING_ID.

    Returns:
      hook: GoogleAdsCustomerMatchHook object.
    """
    hook = ads_cm_hook.GoogleAdsCustomerMatchHook(
        ads_cm_user_list_name=user_list_name,
        ads_upload_key_type=upload_key_type,
        ads_credentials=self.googleads_credentials,
        ads_cm_create_list=create_list,
        ads_cm_membership_lifespan=membership_lifespan,
        ads_cm_app_id=app_id)

    hook.get_user_list_id = mock.MagicMock()
    hook.get_user_list_id.return_value = 1
    hook.create_user_list = mock.MagicMock()
    hook.create_user_list.return_value = 2
    hook.add_members_to_user_list = mock.MagicMock()

    return hook

  def test_ads_cm_hook_invalid_user_list_name(self):
    """Test hook construction with invalid user_list_name."""
    with self.assertRaises(errors.DataOutConnectorValueError):
      self.create_ads_cm_hook(user_list_name='')

  def test_ads_cm_hook_membership_lifespan_less_than_0(self):
    """Test hook construction with invalid membership_lifespan."""
    with self.assertRaises(errors.DataOutConnectorValueError):
      self.create_ads_cm_hook(membership_lifespan=-1)

  def test_ads_cm_hook_membership_lifespan_greater_than_10000(self):
    """Test hook construction with invalid membership_lifespan."""
    with self.assertRaises(errors.DataOutConnectorValueError):
      self.create_ads_cm_hook(membership_lifespan=10001)

  def test_ads_cm_hook_invalid_upload_key_type(self):
    """Test hook construction with invalid upload_key_type."""
    with self.assertRaises(errors.DataOutConnectorError):
      self.create_ads_cm_hook(upload_key_type='invalid upload key type')

  def test_ads_cm_hook_invalid_app_id(self):
    """Test hook construction with invalid app_id."""
    with self.assertRaises(errors.DataOutConnectorValueError):
      self.create_ads_cm_hook(upload_key_type='MOBILE_ADVERTISING_ID',
                              create_list=True)

  def test_ads_cm_hook_send_events_create_new_list(self):
    """Test hook send_events successful."""
    hook = self.create_ads_cm_hook(create_list=True)
    hook.get_user_list_id.side_effect = (
        errors.DataOutConnectorValueError())
    blb = blob.Blob(events=[self.contact_info_event_email], location='')

    blb = hook.send_events(blb)

    self.assertListEqual([], blb.failed_events)
    hook.get_user_list_id.assert_called_once()
    hook.add_members_to_user_list.assert_called_once()

  def test_ads_cm_hook_send_events_create_new_list_is_false(self):
    """Test hook send_events fail due to create_list incorrect value."""
    hook = self.create_ads_cm_hook(create_list=True)
    hook.get_user_list_id.side_effect = (
        errors.DataOutConnectorValueError())
    blb = blob.Blob(events=[self.contact_info_event_email], location='')

    hook = self.create_ads_cm_hook(create_list=False)
    hook.send_events(blb)

    hook.create_user_list.assert_not_called()

  def test_ads_cm_hook_send_events_contact_empty_event(self):
    """Test hook send_events fail due to payload being empty."""
    hook = self.create_ads_cm_hook()
    blb = blob.Blob(events=[{}], location='')

    hook.send_events(blb)
    hook.get_user_list_id.assert_not_called()

  def test_ads_cm_hook_send_events_contact_info_email_only(self):
    """Test hook send_events success with email only payload."""
    hook = self.create_ads_cm_hook()
    blb = blob.Blob(events=[self.contact_info_event_email], location='')

    blb = hook.send_events(blb)

    self.assertListEqual([], blb.failed_events)
    hook.add_members_to_user_list.assert_called()

  def test_ads_cm_hook_send_events_contact_info_bad_email(self):
    """Test hook send_events fail due to incorrect email format."""
    hook = self.create_ads_cm_hook()
    self.contact_info_event_email['hashedEmail'] = 'bad email'
    blb = blob.Blob(events=[self.contact_info_event_email], location='')

    hook.send_events(blb)
    hook.get_user_list_id.assert_not_called()

  def test_ads_cm_hook_send_events_contact_info_phone_number_only(self):
    """Test hook send_events success with phone number only payload."""
    hook = self.create_ads_cm_hook()
    blb = blob.Blob(events=[self.contact_info_event_phone_number], location='')

    blb = hook.send_events(blb)

    self.assertListEqual([], blb.failed_events)
    hook.add_members_to_user_list.assert_called()

  def test_ads_cm_hook_send_events_contact_info_bad_phone_number(self):
    """Test hook send_events fail due to incorrect phone number format."""
    hook = self.create_ads_cm_hook()
    self.contact_info_event_email['hashedPhoneNumber'] = 'bad phone number'
    blb = blob.Blob(events=[self.contact_info_event_email], location='')

    hook.send_events(blb)
    hook.get_user_list_id.assert_not_called()

  def test_ads_cm_hook_send_events_contact_info_with_address_info(self):
    """Test hook send_events success with address info payload."""
    hook = self.create_ads_cm_hook()
    contact_info = {}
    contact_info.update(self.contact_info_event_email)
    contact_info.update(self.address_info)
    blb = blob.Blob(events=[contact_info], location='')

    blb = hook.send_events(blb)

    self.assertListEqual([], blb.failed_events)
    hook.add_members_to_user_list.assert_called()

  def test_ads_cm_hook_send_events_contact_info_with_bad_address_info(self):
    """Test hook send_events fail due to incorrect address info format."""
    hook = self.create_ads_cm_hook()
    contact_info = {}
    contact_info.update(self.contact_info_event_email)
    del self.address_info['zipCode']
    contact_info.update(self.address_info)
    blb = blob.Blob(events=[contact_info], location='')

    blb = hook.send_events(blb)

    self.assertListEqual([], blb.failed_events)
    hook.add_members_to_user_list.assert_called_with(
        1, [self.contact_info_event_email])

  def test_ads_cm_hook_send_events_crm_id_missing_user_id(self):
    """Test hook send_events fail due to missing crm id."""
    hook = self.create_ads_cm_hook(upload_key_type='CRM_ID')
    blb = blob.Blob(events=[{}], location='')

    hook.send_events(blb)
    hook.get_user_list_id.assert_not_called()

  def test_ads_cm_hook_send_events_crm_id(self):
    """Test hook send_events success with crm id."""
    hook = self.create_ads_cm_hook(upload_key_type='CRM_ID')
    blb = blob.Blob(events=[self.crm_id_event], location='')

    blb = hook.send_events(blb)

    self.assertListEqual([], blb.failed_events)
    hook.add_members_to_user_list.assert_called()

  def test_ads_cm_hook_send_events_mobile_advertising_good_id(self):
    """Test hook send_events success with mobile advertising id."""
    hook = self.create_ads_cm_hook(upload_key_type='MOBILE_ADVERTISING_ID')
    blb = blob.Blob(events=[self.mobile_id_event], location='')

    blb = hook.send_events(blb)

    self.assertListEqual([], blb.failed_events)
    hook.add_members_to_user_list.assert_called()

  def test_ads_cm_hook_send_events_mobile_advertising_no_id(self):
    """Test hook send_events fail due to missing mobile advertising id."""
    hook = self.create_ads_cm_hook(upload_key_type='MOBILE_ADVERTISING_ID')
    mobile_id_event = {}
    blb = blob.Blob(events=[mobile_id_event], location='')

    hook.send_events(blb)

    hook.get_user_list_id.assert_not_called()

  def test_ads_cm_hook_send_events_contact_info_add_members_to_list(self):
    """Test hook send_events success with contact info payload."""
    hook = self.create_ads_cm_hook(create_list=True)
    hook.add_members_to_user_list.side_effect = (
        errors.DataOutConnectorSendUnsuccessfulError())
    blb = blob.Blob(events=[self.contact_info_event_email], location='')

    hook = self.create_ads_cm_hook()
    blb = hook.send_events(blb)

    self.assertListEqual([], blb.failed_events)
    hook.add_members_to_user_list.assert_called()


if __name__ == '__main__':
  unittest.main()
