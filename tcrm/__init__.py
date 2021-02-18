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

"""Custom plugins for TCRM (Cloud Composer For Data)."""

from airflow.plugins_manager import AirflowPlugin
from gps_building_blocks.tcrm.hooks.ads_uac_hook import AdsUniversalAppCampaignHook
from gps_building_blocks.tcrm.hooks.ga_hook import GoogleAnalyticsHook
from gps_building_blocks.tcrm.hooks.gcs_hook import GoogleCloudStorageHook
from gps_building_blocks.tcrm.operators.data_connector_operator import DataConnectorOperator


class TCRMPlugin(AirflowPlugin):
  """Custom plugins for TCRM."""
  name = "tcrm_plugins"
  hooks = [AdsUniversalAppCampaignHook, GoogleAnalyticsHook,
           GoogleCloudStorageHook]
  operators = [DataConnectorOperator]
  executors = []
  macros = []
  admin_views = []
  flask_blueprints = []
  menu_links = []
