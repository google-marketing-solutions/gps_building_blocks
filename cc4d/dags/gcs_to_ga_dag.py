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

"""Airflow DAG for CC4D workflow.

This DAG will transfer data from Google Cloud Storage to Google Analytics.

This DAG relies on three Airflow variables:
* `ga_tracking_id`:    Google Analytics Tracking ID: UA-XXXXX-YY.
* `gcs_bucket_name`:   Google Cloud Storage bucket name. Ex: 'my_bucket'.
* `gcs_bucket_prefix`: Google Cloud Storage folder name where data is stored.
                       Ex: 'my_folder'.
Refer to https://airflow.apache.org/concepts.html#variables for more on Airflow
Variables.
"""

import datetime
import logging

import os
from airflow import models
from airflow import utils
from gps_building_blocks.cc4d.operators import gcs_to_ga_initiator_operator

# Airflow configuration variables.
_AIRFLOW_ENV = 'AIRFLOW_HOME'

# Airflow DAG configuration.
_DAG_NAME = 'cc4d_gcs_to_ga'
_DAG_RETRIES = 1
_DAG_RETRY_DELAY = 3  # In minutes.
_DAG_SCHEDULE = '@once'

# Required Google Analytics base parameters. Refer to
# https://developers.google.com/analytics/devguides/collection/protocol/v1/parameters
# for more details on required parameters for Measurement Protocol API.
_GA_BASE_PARAMS = {'v': '1'}


def _initialize_dag() -> models.DAG:
  """Initializes an Airflow DAG with appropriate default args.

  Returns:
    models.DAG: Instance models.DAG.
  """
  dag_schedule = models.Variable.get('schedule', _DAG_SCHEDULE)
  logging.info(f'Running pipeline at schedule {dag_schedule}.')

  default_args = {
      'retries': _DAG_RETRIES,
      'retry_delay': datetime.timedelta(minutes=_DAG_RETRY_DELAY),
      'start_date': utils.dates.days_ago(1)
  }

  return models.DAG(
      dag_id=_DAG_NAME,
      schedule_interval=dag_schedule,
      default_args=default_args)


def create_dag() -> models.DAG:
  """Creates and initializes the DAG.

  Returns:
    main_dag: models.DAG with BigQuery to Google Analytics task.
  """
  main_dag = _initialize_dag()

  # Send data from Cloud Storage to Google Analytics via
  # Measurement Protocol API.
  (gcs_to_ga_initiator_operator
   .GoogleCloudStorageToGoogleAnalyticsInitiatorOperator)(
       task_id='gcs_to_ga_task',
       gcs_bucket=models.Variable.get('gcs_bucket_name', ''),
       gcs_prefix=models.Variable.get('gcs_bucket_prefix', ''),
       ga_tracking_id=models.Variable.get('ga_tracking_id', ''),
       ga_base_params=_GA_BASE_PARAMS,
       dag=main_dag)

  return main_dag


if os.getenv(_AIRFLOW_ENV):
  dag = create_dag()
