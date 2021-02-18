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

"""Tests for tcrm.utils.retry_utils."""

import unittest
from unittest import mock

from airflow.contrib.hooks import bigquery_hook
from airflow.contrib.hooks import gcp_api_base_hook
import parameterized

from gps_building_blocks.cloud.utils import cloud_auth
from gps_building_blocks.tcrm.utils import hook_factory

_HOOKS_KWARGS = {'ads_credentials': 'ads_credentials',
                 'ads_upload_key_type': 'CRM_ID',
                 'ads_cm_app_id': '1',
                 'ads_cm_create_list': True,
                 'ads_cm_membership_lifespan': 8,
                 'ads_cm_user_list_name': 'my_list',
                 'ads_uac_conn_id': 'conn_id',
                 'bq_conn_id': 'conn_id',
                 'bq_dataset_id': 'dataset',
                 'bq_table_id': 'table',
                 'bq_selected_fields': ['f1'],
                 'cm_service_account': 'service_account_name',
                 'cm_profile_id': '123456',
                 'ga_tracking_id': 'UA-12345-6',
                 'ga_base_params': 'ga_base_params',
                 'gcs_bucket': 'bucket',
                 'gcs_content_type': 'JSON',
                 'gcs_prefix': 'prefix'}


def parameterize_function_name(testcase_func, unused_param_num, param):
  """A helper function to parameterizing a given function name.

  Args:
    testcase_func: The function to parameterize its name.
    unused_param_num: Number of parameters in param (unused in this function).
    param: The parameters to add to the function name

  Returns:
    The new function name with parameters in it.
  """
  return '%s_%s' %(testcase_func.__name__,
                   parameterized.parameterized.to_safe_name(
                       '_'.join(str(x) for x in param.args)))


class HookFactoryTest(unittest.TestCase):

  def test_get_input_hook_bigquery(self):
    bigquery_hook_mock = mock.patch.object(
        bigquery_hook.BigQueryHook, '__init__', autospec=True)
    bigquery_hook_mock.start()

    hook = hook_factory.get_input_hook(hook_factory.InputHookType.BIG_QUERY,
                                       **_HOOKS_KWARGS)

    self.assertIsInstance(hook, hook_factory.InputHookType.BIG_QUERY.value)

  def test_get_input_hook_gcs(self):
    with mock.patch.object(gcp_api_base_hook.GoogleCloudBaseHook, '__init__',
                           autospec=True):
      build_impersonated_client_mock = mock.patch.object(
          cloud_auth, 'build_impersonated_client', autospec=True)
      build_impersonated_client_mock.return_value = mock.Mock()
      build_impersonated_client_mock.start()

      hook = hook_factory.get_input_hook(
          hook_factory.InputHookType.GOOGLE_CLOUD_STORAGE, **_HOOKS_KWARGS)

    self.assertIsInstance(
        hook, hook_factory.InputHookType.GOOGLE_CLOUD_STORAGE.value)

  @parameterized.parameterized.expand(
      hook_factory.OutputHookType.__members__.keys(),
      testcase_func_name=parameterize_function_name)
  def test_get_output_hook(self, hook_type):
    hook = hook_factory.get_output_hook(hook_factory.OutputHookType[hook_type],
                                        **_HOOKS_KWARGS)

    self.assertIsInstance(hook, hook_factory.OutputHookType[hook_type].value)


if __name__ == '__main__':
  unittest.main()
