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

"""Airflow DAG for TCRM workflow.

This DAG will transfer data from BigQuery to Google Ads Customer Match.

This DAG relies on these Airflow variables:
* `ads_cm_user_list_name`:      The Customer MAtch user list name.
                                Ex: `my_list`.
* `ads_credentials`:            A dict of Adwords client ids and tokens.
                                Reference for desired format:
                                https://developers.google.com/adwords/api/docs/guides/first-api-call
* `ads_upload_key_type`:        The upload key type. Refer to
                                ads_hook.UploadKeyType for more information.
* `ads_cm_app_id`:              An ID string required for creating a new list if
                                upload_key_type is MOBILE_ADVERTISING_ID.
* `ads_cm_create_list`:         A flag to enable a new list creation if a list
                                called user_list_name doesn't exist.
* `ads_cm_membership_lifespan`: Number of days a user's cookie stays. Refer to
                                ads_hook.GoogleAdsHook for details.
* `bq_dataset_id`:              BigQuery dataset name. Ex: `my_dataset`.
* `bq_table_id`:                BigQuery table name which holds the data.
                                Ex: `my_table`

Refer to https://airflow.apache.org/concepts.html#variables for more on Airflow
Variables.
"""

import os
from airflow import models

from gps_building_blocks.tcrm.dags import base_dag
from gps_building_blocks.tcrm.operators import data_connector_operator
from gps_building_blocks.tcrm.utils import hook_factory

# Airflow configuration variables.
_AIRFLOW_ENV = 'AIRFLOW_HOME'

# Airflow DAG configurations.
_DAG_NAME = 'tcrm_bq_to_ads_cm'

# BigQuery connection ID. Refer to
# https://cloud.google.com/composer/docs/how-to/managing/connections
# for more details on Managing Airflow connections.
_BQ_CONN_ID = 'bigquery_default'

# Membership lifespan controls how many days that a user's cookie stays on your
# list since its most recent addition to the list. Acceptable range is from 0 to
# 10000, and 10000 means no expiration.
_ADS_MEMBERSHIP_LIFESPAN_DAYS = 8


class BigQueryToAdsCMDag(base_dag.BaseDag):
  """BigQuery to Google Ads Customer Match DAG."""

  def create_task(self, main_dag: models.DAG = None, is_retry: bool = False
                 ) -> data_connector_operator.DataConnectorOperator:
    """Creates and initializes the main DAG.

    Args:
      main_dag: The dag that the task attaches to.
      is_retry: Whether or not the operator should include a retry task.

    Returns:
      DataConnectorOperator.
    """
    return data_connector_operator.DataConnectorOperator(
        dag_name=_DAG_NAME,
        task_id=self.get_task_id('bq_to_ads_cm', is_retry),
        input_hook=hook_factory.InputHookType.BIG_QUERY,
        output_hook=hook_factory.OutputHookType.GOOGLE_ADS_CUSTOMER_MATCH,
        is_retry=is_retry,
        return_report=self.dag_enable_run_report,
        enable_monitoring=self.dag_enable_monitoring,
        monitoring_dataset=self.monitoring_dataset,
        monitoring_table=self.monitoring_table,
        monitoring_bq_conn_id=self.monitoring_bq_conn_id,
        bq_conn_id=_BQ_CONN_ID,
        bq_dataset_id=models.Variable.get('bq_dataset_id', ''),
        bq_table_id=models.Variable.get('bq_table_id', ''),
        ads_credentials=models.Variable.get('ads_credentials', ''),
        ads_upload_key_type=models.Variable.get('ads_upload_key_type', ''),
        ads_cm_app_id=models.Variable.get('ads_cm_app_id', None),
        ads_cm_create_list=models.Variable.get('ads_cm_create_list', True),
        ads_cm_membership_lifespan=models.Variable.get(
            'ads_cm_membership_lifespan', _ADS_MEMBERSHIP_LIFESPAN_DAYS),
        ads_cm_user_list_name=models.Variable.get('ads_cm_user_list_name', ''),
        dag=main_dag)


if os.getenv(_AIRFLOW_ENV):
  dag = BigQueryToAdsCMDag(_DAG_NAME).create_dag()
