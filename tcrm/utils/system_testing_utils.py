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
"""Util for system test."""

import datetime
import glob
import json
import os
import subprocess

from google.cloud import bigquery

_AIRFLOW_HOME = 'AIRFLOW_HOME'


def run_shell_cmd(cmd, timeout=300):
  cp = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                      check=True, shell=True, timeout=timeout)
  return cp.stdout.decode('utf-8')


def get_airflow_home():
  airflow_home = os.environ.get(_AIRFLOW_HOME)
  return airflow_home


def get_iso8601_date_str(datetime_obj):
  dts = datetime_obj.astimezone().replace(hour=0, minute=0, second=0,
                                          microsecond=0,
                                          tzinfo=datetime.timezone.utc
                                         ).isoformat()
  return dts


def create_or_update_airflow_gcp_connection(conn_id, project_id, key_path):
  extra = json.dumps({'extra__google_cloud_platform__project': project_id,
                      'extra__google_cloud_platform__key_path': key_path
                     }).replace('"', '\\"')
  run_shell_cmd(f'airflow connections -d --conn_id {conn_id};')
  run_shell_cmd(f'airflow connections -a --conn_id {conn_id}'
                f' --conn_type google_cloud_platform --conn_extra "{extra}"; ')


def create_or_update_airflow_variable(name, value):
  run_shell_cmd(
      f'airflow variables -s {name} {value};')


def run_airflow_task(dag_id, task_id, execution_date):
  run_shell_cmd(f'airflow run {dag_id} {task_id} {execution_date}; ')


def get_latest_task_log(dag_id, task_id, execution_date):
  log_folder = os.path.join(get_airflow_home(), 'logs', dag_id, task_id,
                            execution_date)
  log_files = glob.glob(f'{log_folder}/*')
  latest_log = max(log_files, key=os.path.getctime)
  with open(latest_log, 'r') as f:
    log_content = f.read()
    return log_content


def insert_rows_to_table(rows, table_id):
  client = bigquery.Client()
  errors = client.insert_rows_json(table_id, rows)
  if errors:
    raise RuntimeError(errors)
