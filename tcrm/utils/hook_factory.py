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

"""A TCRM Hook factory.

The factory functions in this util file can create any type of hook given one
of the types in InputHookType or OutputHookType.

Any new TCRM hook should be registered in this file to allow for it's creation.
To add a new hook:
  - Import the hook into this file
  - Add an enum for it either in InputHookType or OutputHookType

Example usage:

  import hook_factory
  bq_hook = hook_factory.get_input_hook(InputHookType.BIG_QUERY,
                                        bq_arg1, bq_arg2,...)
  ga_hook = hook_factory.get_output_hook(OutputHookType.GOOGLE_ANALYTICS,
                                         ga_arg1, ga_arg2, ...)

"""

import enum

from gps_building_blocks.tcrm.hooks import ads_cm_hook
from gps_building_blocks.tcrm.hooks import ads_oc_hook
from gps_building_blocks.tcrm.hooks import ads_uac_hook
from gps_building_blocks.tcrm.hooks import bq_hook
from gps_building_blocks.tcrm.hooks import cm_hook
from gps_building_blocks.tcrm.hooks import ga_hook
from gps_building_blocks.tcrm.hooks import gcs_hook
from gps_building_blocks.tcrm.hooks import input_hook_interface
from gps_building_blocks.tcrm.hooks import output_hook_interface


class InputHookType(enum.Enum):
  BIG_QUERY = bq_hook.BigQueryHook
  GOOGLE_CLOUD_STORAGE = gcs_hook.GoogleCloudStorageHook


class OutputHookType(enum.Enum):
  GOOGLE_ADS_CUSTOMER_MATCH = ads_cm_hook.GoogleAdsCustomerMatchHook
  GOOGLE_ADS_OFFLINE_CONVERSIONS = ads_oc_hook.GoogleAdsOfflineConversionsHook
  GOOGLE_ADS_UNIVERSAL_APP_CAMPAIGN = ads_uac_hook.AdsUniversalAppCampaignHook
  GOOGLE_ANALYTICS = ga_hook.GoogleAnalyticsHook
  GOOGLE_CAMPAIGN_MANAGER_OFFLINE_CONVERSIONS = cm_hook.CampaignManagerHook


def get_input_hook(hook_type: InputHookType,
                   **kwargs) -> input_hook_interface.InputHookInterface:
  """Creates an input hook of type hook_type.

  Args:
    hook_type: The type of the hook to create.
    **kwargs: Arguments needed for creating the hook.

  Returns:
    A a hook of type hook_type.
  """
  return hook_type.value(**kwargs)


def get_output_hook(hook_type: OutputHookType,
                    **kwargs) -> output_hook_interface.OutputHookInterface:
  """Creates an output hook of type hook_type.

  Args:
    hook_type: The type of the hook to create.
    **kwargs: Arguments needed for creating the hook.

  Returns:
    A a hook of type hook_type.
  """
  return hook_type.value(**kwargs)
