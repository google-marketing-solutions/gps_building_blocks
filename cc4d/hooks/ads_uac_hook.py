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

"""Custom hook for Google Ads UAC.

For UAC details refer to
https://developers.google.com/adwords/api/docs/guides/mobile-app-campaigns

"""

import enum
import json
import re
from typing import Any, Dict, List, Optional, Text, Tuple
import urllib.parse

from airflow.hooks import http_hook

from gps_building_blocks.cc4d.hooks import output_hook_interface
from gps_building_blocks.cc4d.utils import async_utils
from gps_building_blocks.cc4d.utils import errors

# RDID (raw device id) should be in UUID format.
_RDID_PATTERN = '^[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}$'
_RDID_REGEX = re.compile(_RDID_PATTERN, re.IGNORECASE)
_APP_CONVERSION_TRACKING_PATH = 'pagead/conversion/app/1.0'

_REQUIRED_FIELDS = ('dev_token',
                    'link_id',
                    'app_event_type',
                    'rdid',
                    'id_type',
                    'lat',
                    'app_version',
                    'os_version',
                    'sdk_version',
                    'timestamp')


class AppEventType(enum.Enum):
  FIRST_OPEN = 'first_open'
  SESSION_START = 'session_start'
  IN_APP_PURCHASE = 'in_app_purchase'
  VIEW_ITEM_LIST = 'view_item_list'
  VIEW_ITEM = 'view_item'
  VIEW_SEARCH_RESULTS = 'view_search_results'
  ADD_TO_CART = 'add_to_cart'
  ECOMMERCE_PURCHASE = 'ecommerce_purchase'
  CUSTOM = 'custom'


class IdType(enum.Enum):
  ANDROID = 'advertisingid'
  IOS = 'idfa'


class AdsUACHook(
    http_hook.HttpHook, output_hook_interface.OutputHookInterface):
  """Custom hook for Google Ads UAC API.

  API SPEC for Apps Conversion Tracking and Remarketing
  https://developers.google.com/app-conversion-tracking/api/request-response-specs

  """

  def __init__(self,
               conn_id: Text = 'google_ads_uac_default',
               dry_run: bool = False) -> None:
    super().__init__(http_conn_id=conn_id)
    self.dry_run = dry_run

  def _get_developer_token(self) -> Text:
    """Gets developer token from connection configuration.

    Returns:
      dev_token: Developer token of Google Ads API.

    Raises:
      DataOutConnectorValueError: If connection is not available or if password
      is missing in the connection.
    """
    conn = self.get_connection(self.http_conn_id)
    if not conn:
      raise errors.DataOutConnectorValueError(
          'Cannot get connection {id}.'.format(id=self.http_conn_id))
    if not conn.password:
      raise errors.DataOutConnectorValueError(
          'Missing dev token. Please check connection {id} and its password.'
          .format(id=self.http_conn_id))
    return conn.password

  def _validate_app_conversion_payload(self, payload: Dict[Text, Any]) -> None:
    """Validates payload sent to UAC.

    Args:
      payload: The payload to be validated before sending to Google Ads UAC.

    Raises:
      DataOutConnectorValueError: If some value is missing or in wrong format.
    """

    for key in _REQUIRED_FIELDS:
      if payload.get(key) is None:
        raise errors.DataOutConnectorValueError(
            """Missing {key} in payload.""".format(key=key))

    if payload.get('app_event_type') not in [item.value
                                             for item in AppEventType]:
      raise errors.DataOutConnectorValueError("""Unsupported app event type in
          payload. Example: 'first_open', 'session_start', 'in_app_purchase',
          'view_item_list', 'view_item', 'view_search_results',
          'add_to_cart', 'ecommerce_purchase', 'custom'.""")

    if (payload.get('app_event_name') and
        payload.get('app_event_type') != 'custom'):
      raise errors.DataOutConnectorValueError(
          """App event type must be 'custom' when app event name exists.""")

    match = _RDID_REGEX.match(payload.get('rdid'))
    if not match:
      raise errors.DataOutConnectorValueError("""Wrong raw device id format in
          payload. Should be compatible with RFC4122.""")

    if payload.get('id_type') not in [item.value for item in IdType]:
      raise errors.DataOutConnectorValueError("""Wrong raw device id type in
          payload. Example: 'advertisingid', 'idfa'.""")

    if payload.get('lat') != 0 and payload.get('lat') != 1:
      raise errors.DataOutConnectorValueError("""Wrong limit-ad-tracking status
          in payload. Example: 0, 1.""")

  def send_conversions_to_uac(
      self, params: Dict[Text, Any]) -> Optional[Dict[Text, Any]]:
    """Sends conversion to UAC via S2S REST API.

    Args:
      params: Parameters containing required data for app conversion tracking.

    Returns:
      results: Includes request body, status_code, error_msg, response body and
      dry_run flag.
      The response refers to the definition of conversion tracking response in
      https://developers.google.com/app-conversion-tracking/api/request-response-specs#conversion_tracking_response
    """
    try:
      request_params = dict(params)
      request_params['dev_token'] = self._get_developer_token()
      app_event_data = request_params.get('app_event_data')

      if 'app_event_data' in request_params:
        del request_params['app_event_data']
      self._validate_app_conversion_payload(request_params)
    except errors.DataOutConnectorValueError as error:
      self.log.exception(error)
      return {'request': params,
              'status_code': 400,
              'error_msg': str(error),
              'dry_run': self.dry_run}

    self.method = 'POST'
    query_url = urllib.parse.urlencode(request_params)
    complete_url = ('{path}?{default_query}'
                    .format(
                        path=_APP_CONVERSION_TRACKING_PATH,
                        default_query=query_url))

    if self.dry_run:
      self.log.debug(
          """Dry run mode: Sending conversion tracking data to UAC.
          URL:{}. App event data:{}."""
          .format(complete_url, json.dumps(app_event_data)))
      return {'request': params,
              'status_code': 500,
              'error_msg': 'Dry run mode',
              'dry_run': self.dry_run}

    response = None
    extra_options = {'check_response': False}
    self.log.info(
        """Not Dry run mode: Sending conversion tracking data to UAC.
        URL:{}. App event data:{}."""
        .format(complete_url, json.dumps(app_event_data)))
    response = self.run(endpoint=complete_url,
                        data=app_event_data,
                        extra_options=extra_options)
    try:
      body = response.json()
      return {'request': params,
              'status_code': response.status_code,
              'response': body,
              'dry_run': self.dry_run}
    except (ValueError, KeyError, TypeError):
      return {'request': params,
              'status_code': response.status_code,
              'error_msg': response.reason,
              'dry_run': self.dry_run}

  def send_events(self, events: List[Dict[Any, Any]]
                 ) -> Tuple[List[int], List[int], List[Dict[Any, Any]]]:
    """Sends all events to the Google Ads UAC API.

    Args:
      events: Events to send to Google Ads UAC.

    Returns:
      report: A tuple containing the following three elements.
        successful_indexes: List of indexes of successfully sent events.
        unsuccessful_indexes: List of indexes of unsuccessfully sent events.
        responses: List of response of Google Ads UAC API.
    """
    params_list = [{'params': event} for event in events]
    results = async_utils.run_synchronized_function(
        self.send_conversions_to_uac, params_list)

    successfully_sent_event_indexes = []
    unsuccessfully_sent_event_indexes = []
    for i, result in enumerate(results):
      if isinstance(result, Dict) and result.get('response'):
        successfully_sent_event_indexes.append(i)
      else:
        unsuccessfully_sent_event_indexes.append(i)

    return (successfully_sent_event_indexes,
            unsuccessfully_sent_event_indexes,
            list(results))
