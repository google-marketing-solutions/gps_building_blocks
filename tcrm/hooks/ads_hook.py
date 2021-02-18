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

"""Custom hook for Google Ads.

For Google Ads via the Adwords API details refer to:
https://developers.google.com/adwords/api/docs/reference/release-notes/v201809

A dict with the required tokens for Google Ads Authentication is needed.
See required format here:
https://developers.google.com/adwords/api/docs/guides/first-api-call#get_an_oauth2_refresh_token_and_configure_your_client
"""

import enum
from typing import Any, Dict, List, Mapping, Text

from airflow.hooks import base_hook
from google.auth import exceptions as google_auth_exceptions
from googleads import adwords
from googleads import common
from googleads import errors as googleads_errors

from gps_building_blocks.tcrm.utils import errors


_API_VERSION = 'v201809'
_DEFAULT_BATCH_SIZE = 1000

# Membership lifespan controls how many days that a user's cookie stays on your
# list since its most recent addition to the list. Acceptable range is from 0 to
# 10000, and 10000 means no expiration.
MEMBERSHIP_LIFESPAN_DAYS = 8


class ServiceType(enum.Enum):
  """A enum class for listing available Google Adwords Services.

     For more detail:
     https://developers.google.com/adwords/api/docs/reference/release-notes/v201809
  """
  ADWORDS_USER_LIST_SERVICE = 'AdwordsUserListService'
  OFFLINE_CONVERSION_FEED_SERVICE = 'OfflineConversionFeedService'


class UploadKeyType(enum.Enum):
  """An enum class for listing available upload key types.

  Upload key type is used for how to match the member.
  Available types are:
    1) CONTACT_INFO: Members are matched from customer info such as email
    address, phone number or physical address.
    2) CRM_ID: Members are matched from advertiser generated and assigned user
    ID.
    3) MOBILE_ADVERTISING_ID: Members are matched from mobile advertising IDs.

    For more detail:
    https://developers.google.com/adwords/api/docs/reference/v201809/AdwordsUserListService.CrmBasedUserList
  """
  CONTACT_INFO = enum.auto()
  CRM_ID = enum.auto()
  MOBILE_ADVERTISING_ID = enum.auto()


class GoogleAdsHook(base_hook.BaseHook):
  """Custom hook for Google Ads via Adwords API."""

  def __init__(self, ads_yaml_doc: Text,
               ads_api_version: Text = _API_VERSION, **kwargs) -> None:
    """Initializes an Adwords client with specified configurations.

    Args:
      ads_yaml_doc: A dict with Credentials for Google Ads Authentication.
      ads_api_version: Indicates which version of the Google Adwords API to use.
      **kwargs: Other optional arguments.
    """
    self.api_version = ads_api_version
    self.yaml_doc = ads_yaml_doc

  def _get_service(
      self, service_type: ServiceType,
      enable_partial_failure: bool = False) -> common.GoogleSoapService:
    """Gets AdWords service according to the given service type.

    Partial failure detailed explanation:
    https://developers.google.com/adwords/api/docs/guides/partial-failure

    Args:
      service_type: AdWords service to create a service client for. See all
        available services in ServiceType.
      enable_partial_failure: A flag to allow request that valid operations be
        committed and failed ones return errors.

    Returns:
      AdWords service object.

    Raises:
      DataOutConnectorAuthenticationError raised when authentication errors
      occurred.
      DataOutConnectorValueError if the service can't be created.
    """
    try:
      adwords_client = adwords.AdWordsClient.LoadFromString(self.yaml_doc)
      adwords_client.partial_failure = enable_partial_failure
    except googleads_errors.GoogleAdsValueError as error:
      raise errors.DataOutConnectorAuthenticationError(
          error=error,
          msg=('Please check the credentials in the yml doc, it should contains'
               ' a top level key named adwords and 5 sub key-value'
               ' pairs named client_customer_id, developer_token, client_id,'
               ' client_secret and refresh_token.'),
          error_num=(errors.ErrorNameIDMap.
                     RETRIABLE_ERROR_OUTPUT_AUTHENTICATION_FAILED))

    try:
      service = adwords_client.GetService(service_type.value, self.api_version)
    except googleads_errors.GoogleAdsValueError as error:
      raise errors.DataOutConnectorValueError(
          error=error, msg='Couldn\'t get service from Google Adwords API',
          error_num=errors.ErrorNameIDMap
          .RETRIABLE_ADS_HOOK_ERROR_UNAVAILABLE_ADS_SERVICE)
    return service

  def get_user_list_id(self, user_list_name: Text) -> int:
    """Converts user list name to user list ID.

    Searches for a ServiceType.AdwordsUserListService list in Google Ads and
    returns the list's ID if it exists and raises an error if it doesn't exist.

    Args:
      user_list_name: The name of the user list to get the ID for.

    Returns:
      user_list_id: ID of the user list.

    Raises:
      DataOutConnectorAuthenticationError raised when authentication errors
      occurred.
      DataOutConnectorValueError if the list with given user list name doesn't
      exist.
    """
    user_list_meta_data_selector = {
        'fields': ['Name', 'Id'],
        'predicates': [{
            'field': 'Name',
            'operator': 'EQUALS',
            'values': user_list_name
        }, {
            'field': 'ListType',
            'operator': 'EQUALS',
            'values': 'CRM_BASED'
        }],
    }
    service = self._get_service(ServiceType.ADWORDS_USER_LIST_SERVICE)

    try:
      result = service.get(user_list_meta_data_selector)
    except (googleads_errors.GoogleAdsServerFault,
            googleads_errors.GoogleAdsValueError,
            google_auth_exceptions.RefreshError) as error:
      raise errors.DataOutConnectorAuthenticationError(
          error=error,
          msg='Failed to get user list ID due to authentication error.',
          error_num=(errors.ErrorNameIDMap.
                     RETRIABLE_ERROR_OUTPUT_AUTHENTICATION_FAILED))

    if 'entries' in result and len(result['entries']):
      user_list_id = result['entries'][0]['id']
    else:
      raise errors.DataOutConnectorValueError(
          msg="""Failed to get user list ID. List doesn't exist""",
          error_num=errors.ErrorNameIDMap
          .ADS_HOOK_ERROR_FAIL_TO_GET_USER_LIST_ID)

    return user_list_id

  def create_user_list(
      self,
      user_list_name: Text,
      upload_key_type: UploadKeyType = UploadKeyType.CONTACT_INFO,
      membership_lifespan: int = MEMBERSHIP_LIFESPAN_DAYS,
      app_id: Text = None) -> int:
    """Creates a new user list.

    Args:
      user_list_name: The name of the user list to upload.
      upload_key_type: One of the keys listed in UploadKeyType.
      membership_lifespan: Number of days a user's cookie stays.
      app_id: Mobile app id for creating user list.

    Returns:
      The ID of the new user list.

    Raises:
      DataOutConnectorAuthenticationError raised when authentication errors
      occurred.
      DataOutConnectorValueError if a new user list cannot be created.
    """
    service = self._get_service(ServiceType.ADWORDS_USER_LIST_SERVICE)
    new_user_list = {
        'xsi_type': 'CrmBasedUserList',
        'name': user_list_name,
        'description': 'A list of users uploaded from Adwords API via TCRM',
        'membershipLifeSpan': membership_lifespan,
        'uploadKeyType': upload_key_type.name,
    }

    operations = [{'operator': 'ADD', 'operand': new_user_list}]
    try:
      result = service.mutate(operations)
    except (googleads_errors.GoogleAdsServerFault,
            googleads_errors.GoogleAdsValueError,
            google_auth_exceptions.RefreshError) as error:
      raise errors.DataOutConnectorAuthenticationError(
          error=error,
          msg='Failed to create user list due to authentication error.',
          error_num=(errors.ErrorNameIDMap.
                     RETRIABLE_ERROR_OUTPUT_AUTHENTICATION_FAILED))

    if (upload_key_type == UploadKeyType.MOBILE_ADVERTISING_ID and
        app_id is not None):
      new_user_list['appId'] = app_id

    if 'value' in result and len(result['value']):
      return result['value'][0]['id']
    else:
      raise errors.DataOutConnectorError(
          msg='Failed to create user list. (response error)',
          error_num=(errors.ErrorNameIDMap.
                     RETRIABLE_ADS_HOOK_ERROR_FAIL_CREATING_USER_LIST))

  def add_members_to_user_list(self, user_list_id: int,
                               payload: List[Dict[str, Any]]) -> None:
    """Adds new members to a Google Ads user list.

    Args:
      user_list_id: The ID of the user list to upload.
      payload: A batch of payload data that will be sent to AdWords API.

    Raises:
      DataOutConnectorAuthenticationError raised when authentication errors
      occurred.
      DataOutConnectorSendUnsuccessfulError if the member list uploaded haven't
      been processed successfully by the API.
    """
    service = self._get_service(ServiceType.ADWORDS_USER_LIST_SERVICE)
    mutate_members_operation = {
        'operand': {
            'userListId': user_list_id,
            'membersList': payload
        },
        'operator': 'ADD'
    }

    try:
      response = service.mutateMembers([mutate_members_operation])
    except(googleads_errors.GoogleAdsServerFault,
           google_auth_exceptions.RefreshError) as error:
      raise errors.DataOutConnectorAuthenticationError(
          error=error,
          msg='Failed to add members to user list due to authentication error.',
          error_num=(errors.ErrorNameIDMap.
                     RETRIABLE_ERROR_OUTPUT_AUTHENTICATION_FAILED))

    try:
      if response['userLists'][0]['dataUploadResult'] == 'FAILURE':
        raise errors.DataOutConnectorSendUnsuccessfulError(
            'Failed to add members to the user list.',
            error_num=errors.ErrorNameIDMap
            .RETRIABLE_ADS_HOOK_ERROR_FAIL_ADDING_MEMBERS_TO_USER_LIST)
    except (KeyError, IndexError):
      raise errors.DataOutConnectorSendUnsuccessfulError(
          'Failed to add members to the user list.',
          error_num=errors.ErrorNameIDMap
          .RETRIABLE_ADS_HOOK_ERROR_FAIL_ADDING_MEMBERS_TO_USER_LIST)

  def add_offline_conversions(
      self, payload: List[Mapping[Text, Any]]) -> Dict[str, Any]:
    """Uploads offline conversions.

    The uploaded results represents as partial failures. For detail:
    https://developers.google.com/adwords/api/docs/guides/partial-failure

    Args:
      payload: The offline conversions to uplad.

    Returns:
      The partial failures.

    Raises:
      DataOutConnectorAuthenticationError raised when authentication errors
      occurred.
    """
    service = self._get_service(
        ServiceType.OFFLINE_CONVERSION_FEED_SERVICE, True)
    try:
      partial_failures = service.mutate(payload)
    except(googleads_errors.GoogleAdsServerFault,
           google_auth_exceptions.RefreshError) as error:
      raise errors.DataOutConnectorAuthenticationError(
          error=error,
          msg='Failed to add offline conversions due to authentication error.',
          error_num=(errors.ErrorNameIDMap.
                     RETRIABLE_ERROR_OUTPUT_AUTHENTICATION_FAILED))
    return partial_failures
