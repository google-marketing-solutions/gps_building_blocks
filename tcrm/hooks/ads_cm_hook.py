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

"""Custom hook for Google Ads Customer Match.

For customer match details refer to
https://developers.google.com/google-ads/api/docs/remarketing/audience-types/customer-match
"""
import re
from typing import Any, Callable, Dict, List, Tuple, Generator

from gps_building_blocks.tcrm.hooks import ads_hook
from gps_building_blocks.tcrm.hooks import output_hook_interface
from gps_building_blocks.tcrm.utils import blob
from gps_building_blocks.tcrm.utils import errors


_DEFAULT_BATCH_SIZE = 1000
_SHA256_DIGEST_PATTERN = r'^[A-Fa-f0-9]{64}$'


def _validate_sha256_pattern(field_data: str) -> None:
  """Validates if field_data matches sha256 digest string pattern.

  The correct patterh is '^[A-Fa-f0-9]{64}$'
  Note: None is an invalid sha256 value

  Args:
    field_data: A field data which is a part of member data entity of Google
                Adwords API

  Raises:
    DataOutConnectorValueError: If the any field data is invalid or None.
  """
  if field_data is None or not re.match(_SHA256_DIGEST_PATTERN, field_data):
    raise errors.DataOutConnectorValueError(
        'None or string is not in SHA256 format.',
        errors.ErrorNameIDMap
        .ADS_CM_HOOK_ERROR_PAYLOAD_FIELD_VIOLATES_SHA256_FORMAT)


def _is_address_info_available(event: Dict[Any, Any]) -> bool:
  """Check if address info needs to be added to formatted event.

  Args:
    event: the raw event from data source.

  Returns:
    bool to indicate that address info is available.
  """
  keys_exist = all(k in event for k in (
      'hashedFirstName', 'hashedLastName', 'countryCode', 'zipCode'))
  if not keys_exist:
    return False
  values_exist = all((event['hashedFirstName'], event['hashedLastName'],
                      event['countryCode'], event['zipCode']))
  if not values_exist:
    return False
  return True


def _format_contact_info_event(event: Dict[Any, Any]) -> Dict[Any, Any]:
  """Format a contact_info event.

  Args:
    event: A raw contact_info event.

  Returns:
    A formatted contact_info event.

  Raises:
    DataOutConnectorValueError for the following scenarios:
      - If filed hashedEmail and hashedPhoneNumber not
        exist in the payload.
      - hashedEmail or hashedPhoneNumber fields do not meet SHA256 format.
  """
  member = {}

  if event.get('hashedEmail', None) is not None:
    _validate_sha256_pattern(event.get('hashedEmail', None))
    member['hashedEmail'] = event['hashedEmail']

  if event.get('hashedPhoneNumber', None) is not None:
    _validate_sha256_pattern(event.get('hashedPhoneNumber', None))
    member['hashedPhoneNumber'] = event['hashedPhoneNumber']

  if 'hashedEmail' not in member and 'hashedPhoneNumber' not in member:
    raise errors.DataOutConnectorValueError(
        'Data must contain either a valid hashed email or phone number.',
        errors.ErrorNameIDMap.ADS_CM_HOOK_ERROR_INVALID_EMAIL_AND_PHONE_NUMBER)

  if _is_address_info_available(event):
    hashed_first_name = event['hashedFirstName']
    _validate_sha256_pattern(hashed_first_name)
    hashed_last_name = event['hashedLastName']
    _validate_sha256_pattern(hashed_last_name)
    member['addressInfo'] = {
        'hashedFirstName': hashed_first_name,
        'hashedLastName': hashed_last_name,
        'countryCode': event['countryCode'],
        'zipCode': event['zipCode'],
    }
  return member


def _format_crm_id_event(event: Dict[Any, Any]) -> Dict[Any, Any]:
  """Format a crm_id event.

  Args:
    event: A raw crm_id event.

  Returns:
    A formatted crm_id event.

  Raises:
    DataOutConnectorValueError if userId is not exist in the event.
  """
  if 'userId' not in event:
    raise errors.DataOutConnectorValueError(
        """userId doesn't exist in crm_id event.""",
        errors.ErrorNameIDMap.ADS_CM_HOOK_ERROR_MISSING_USERID_IN_CRMID_EVENT)
  member = {'userId': event['userId']}
  return member


def _format_mobile_advertising_event(event: Dict[Any, Any]) -> Dict[Any, Any]:
  """Format a mobile_advertising_event event.

  Args:
    event: A raw mobile_advertising_event event.

  Returns:
    A formatted mobile_advertising_event event.

  Raises:
    DataOutConnectorValueError if mobileId field doesn't exist in the event.
  """
  if 'mobileId' not in event:
    raise errors.DataOutConnectorValueError(
        'mobileId field doesn\'t exist in the event.',
        errors.ErrorNameIDMap.ADS_CM_HOOK_ERROR_MISSING_MOBILEID_IN_EVENT)
  member = {'mobileId': event['mobileId']}
  return member


class GoogleAdsCustomerMatchHook(
    ads_hook.GoogleAdsHook, output_hook_interface.OutputHookInterface):
  """Custom hook for Google Ads Customer Match API.

  Sample code for AdWords Customer Match API can be found here.
  https://github.com/google/customer-match-upload-script/blob/master/create_and_populate_list.py

  """

  def __init__(
      self, ads_cm_user_list_name: str,
      ads_upload_key_type: str,
      ads_credentials: str,
      ads_cm_membership_lifespan: int = ads_hook.MEMBERSHIP_LIFESPAN_DAYS,
      ads_cm_create_list: bool = True,
      ads_cm_app_id: str = None,
      **kwargs) -> None:
    """Initialize with a specified user_list_name.

    Args:
      ads_cm_user_list_name: The name of the user list to add members to.
      ads_upload_key_type: The upload key type. Refer to ads_hook.UploadKeyType
        for more information.
      ads_credentials: A dict of Adwords client ids and tokens.
        Reference for desired format:
          https://developers.google.com/adwords/api/docs/guides/first-api-call
      ads_cm_membership_lifespan: Number of days a user's cookie stays. Refer to
        ads_hook.GoogleAdsHook for details.
      ads_cm_create_list: A flag to enable a new list creation if a list called
        user_list_name doesn't exist.
      ads_cm_app_id: An ID required for creating a new list if upload_key_type
        is MOBILE_ADVERTISING_ID.
      **kwargs: Other optional arguments.

    Raises:
      DataOutConnectorValueError if any of the following happens.
        - user_list_name is null.
        - membership_lifespan is negative or bigger than 10000.
        - upload_key_type is not supported by ads_hook.
          - app_id is not specificed when create_list = True and upload_key_type
            is MOBILE_ADVERTISING_ID.
    """
    super().__init__(ads_yaml_doc=ads_credentials)

    self._validate_init_params(ads_cm_user_list_name,
                               ads_cm_membership_lifespan)
    self.user_list_name = ads_cm_user_list_name
    self.membership_lifespan = ads_cm_membership_lifespan

    self.create_list = ads_cm_create_list

    self.upload_key_type = self._validate_and_set_upload_key_type(
        ads_upload_key_type, ads_cm_app_id)
    self.app_id = ads_cm_app_id

    self._format_event = self._select_format_event()

  def _validate_init_params(
      self, user_list_name: str, membership_lifespan: int) -> None:
    """Validate user_list_name and membership_lifespan parameters.

    Args:
      user_list_name: The name of the user list to add members to.
      membership_lifespan: Number of days a user's cookie stays.

    Raises:
      DataOutConnectorValueError if user_list_name is null or
      membership_lifespan is negative or bigger than 10000.
    """
    if not user_list_name:
      raise errors.DataOutConnectorValueError(
          'User list name is empty.',
          errors.ErrorNameIDMap.ADS_CM_HOOK_ERROR_EMPTY_USER_LIST_NAME)
    if membership_lifespan < 0 or membership_lifespan > 10000:
      raise errors.DataOutConnectorValueError(
          'Membership lifespan is not between 0 and 10,000.',
          errors.ErrorNameIDMap.ADS_CM_HOOK_ERROR_INVALID_MEMBERSHIP_LIFESPAN)

  def _validate_and_set_upload_key_type(
      self, upload_key_type: str, app_id: str) -> ads_hook.UploadKeyType:
    """Validate upload_key_type and the subsequent parameters for each key type.

    Args:
      upload_key_type: The upload key type. Refer to ads_hook.UploadKeyType for
        more information.
      app_id: An ID required for creating a new list if upload_key_type is
        MOBILE_ADVERTISING_ID.

    Returns:
      UploadKeyType: An UploadKeyType object defined in ads_hook.

    Raises:
      DataOutConnectorValueError in the following scenarios:
        - upload_key_type is not supported by ads_hook.
        - app_id is not specificed when create_list = True and upload_key_type
            is MOBILE_ADVERTISING_ID.
    """
    try:
      validated_upload_key_type = ads_hook.UploadKeyType[upload_key_type]
    except KeyError:
      raise errors.DataOutConnectorValueError(
          'Invalid upload key type. See ads_hook.UploadKeyType for details',
          errors.ErrorNameIDMap.ADS_CM_HOOK_ERROR_INVALID_UPLOAD_KEY_TYPE)

    if (validated_upload_key_type ==
        ads_hook.UploadKeyType.MOBILE_ADVERTISING_ID and self.create_list and
        not app_id):
      raise errors.DataOutConnectorValueError(
          'app_id needs to be specified for '
          'MOBILE_ADVERTISING_ID when create_list is True.',
          errors.ErrorNameIDMap.ADS_CM_HOOK_ERROR_MISSING_APPID)

    return validated_upload_key_type

  def _select_format_event(self) -> Callable[[Dict[Any, Any]], Dict[Any, Any]]:
    """select how to format events based on upload_key_type.

    Returns:
      A formatting function that corresponds to the given upload_key_type.
    """
    format_event_dict = {
        ads_hook.UploadKeyType.CONTACT_INFO:
            _format_contact_info_event,
        ads_hook.UploadKeyType.CRM_ID:
            _format_crm_id_event,
        ads_hook.UploadKeyType.MOBILE_ADVERTISING_ID:
            _format_mobile_advertising_event
    }
    return format_event_dict[self.upload_key_type]

  def _validate_and_prepare_events_to_send(
      self, events: List[Dict[str, Any]]
      ) -> Tuple[List[Tuple[int, Dict[str, Any]]],
                 List[Tuple[int, errors.ErrorNameIDMap]]]:
    """Converts events to correct format before sending.

    Reference for the correct format:
    https://developers.google.com/adwords/api/docs/reference/v201809/AdwordsUserListService.Member

    Args:
      events: All unformated events.

    Returns:
      members: Formated events.
    """
    valid_events = []
    invalid_indices_and_errors = []

    for i, event in enumerate(events):
      try:
        payload = self._format_event(event)
      except errors.DataOutConnectorValueError as error:
        invalid_indices_and_errors.append((i, error.error_num))
      else:
        valid_events.append((i, payload))

    return valid_events, invalid_indices_and_errors

  def _batch_generator(
      self, events: List[Tuple[int, Dict[str, Any]]]
      ) -> Generator[List[Tuple[int, Dict[str, Any]]], None, None]:
    """Splits conversion events into batches of _CONVERSION_BATCH_MAX_SIZE.

    AdWords API batch constraints can be found at:
    https://developers.google.com/adwords/api/docs/reference/v201809/AdwordsUserListService.MutateMembersOperand

    Args:
      events: Indexed events to send.

    Yields:
      List of batches of events. Each batch is of _CONVERSION_BATCH_MAX_SIZE.
    """
    for i in range(0, len(events), _DEFAULT_BATCH_SIZE):
      yield events[i : i + _DEFAULT_BATCH_SIZE]

  def send_events(self, blb: blob.Blob) -> blob.Blob:
    """Sends Customer Match events to Google AdWords API.

    Args:
      blb: A blob containing Customer Match data to send.

    Returns:
      A blob containing updated data about any failing events or reports.

    Raises:
      DataOutConnectorValueError when user list with given name doesn't exist
      and create_list is false.
    """
    user_list_id = None
    valid_events, invalid_indices_and_errors = (
        self._validate_and_prepare_events_to_send(blb.events))
    batches = self._batch_generator(valid_events)

    for batch in batches:
      if not user_list_id:
        try:
          user_list_id = self.get_user_list_id(self.user_list_name)
        except errors.DataOutConnectorValueError:
          if self.create_list:
            user_list_id = self.create_user_list(
                self.user_list_name,
                self.upload_key_type,
                self.membership_lifespan,
                self.app_id)
          else:
            raise errors.DataOutConnectorValueError(
                'user_list_name does NOT exist (create_list = False).')
      try:
        user_list = [event[1] for event in batch]
        self.add_members_to_user_list(user_list_id, user_list)
      except errors.DataOutConnectorSendUnsuccessfulError as error:
        for event in batch:
          invalid_indices_and_errors.append((event[0], error.error_num))

    for event in invalid_indices_and_errors:
      blb.append_failed_event(event[0] + blb.position, blb.events[event[0]],
                              event[1].value)

    return blb
