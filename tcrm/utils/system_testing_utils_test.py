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

"""Tests for tcrm.utils.system_testing_utils."""

import datetime
import os
import shutil
import subprocess
import time
import unittest
import unittest.mock

from gps_building_blocks.tcrm.utils import system_testing_utils


def _create_temp_file(file_path, content):
  file_dir = os.path.dirname(file_path)
  if not os.path.exists(file_dir):
    os.mkdir(file_dir)
  with open(file_path, 'w') as f:
    f.write(content)


class SystemVerificationUtilsTest(unittest.TestCase):

  def test_run_shell_cmd_timed_out(self):
    with self.assertRaises(subprocess.TimeoutExpired):
      system_testing_utils.run_shell_cmd('sleep 2', timeout=1)

  def test_run_shell_exit_code_is_not_zero(self):
    with self.assertRaises(subprocess.CalledProcessError):
      system_testing_utils.run_shell_cmd('exit 1')

  def test_get_iso8601_date_str(self):
    now = datetime.datetime.now()
    ret = system_testing_utils.get_iso8601_date_str(now)
    self.assertEqual(
        ret,
        f'{now.year}-{"{:02d}".format(now.month)}-'
        f'{"{:02d}".format(now.day)}T00:00:00+00:00')

  def test_get_airflow_home(self):
    with unittest.mock.patch.dict('os.environ',
                                  {'AIRFLOW_HOME': '/airflow_home'}):
      ret = system_testing_utils.get_airflow_home()
      self.assertEqual(ret, '/airflow_home')

  def test_create_or_update_airflow_gcp_connection(self):
    with unittest.mock.patch(
        'plugins.pipeline_plugins.utils.system_testing_utils.'
        'run_shell_cmd') as p:
      system_testing_utils.create_or_update_airflow_gcp_connection(
          'conn_id', 'project_id', 'key_path')
      self.assertEqual(p.call_count, 2)

  def test_create_or_update_airflow_variable(self):
    with unittest.mock.patch(
        'plugins.pipeline_plugins.utils.system_testing_utils.'
        'run_shell_cmd') as p:
      system_testing_utils.create_or_update_airflow_variable(
          'key', 'value')
      p.assert_called_once()

  def test_run_airflow_task(self):
    with unittest.mock.patch(
        'plugins.pipeline_plugins.utils.system_testing_utils.'
        'run_shell_cmd') as p:
      system_testing_utils.run_airflow_task('dag_id', 'task_id',
                                            '2020-10-13T00:00:00+00:00')
      p.assert_called_once()

  def test_get_latest_task_log(self):
    airflow_home = '.'
    with unittest.mock.patch.dict('os.environ', {'AIRFLOW_HOME': airflow_home}):
      dag_id = 'tcrm_bq_to_ga'
      task_id = 'bq_to_ga_task'
      execution_date = '2020-10-13T00:00:00+00:00'

      temp_logs_dir = (f'{airflow_home}/logs/{dag_id}/'
                       f'{task_id}/{execution_date}')
      os.makedirs(temp_logs_dir)
      _create_temp_file(os.path.join(temp_logs_dir, '1.log'), 'test1')
      time.sleep(0.1)
      _create_temp_file(os.path.join(temp_logs_dir, '2.log'), 'test2')

      log_content = system_testing_utils.get_latest_task_log(
          dag_id, task_id, execution_date)
      self.assertEqual(log_content, 'test2')
      shutil.rmtree(f'{airflow_home}/logs')

  @unittest.mock.patch('google.cloud.bigquery.Client')
  def test_insert_rows_to_table(self, mock_client_class):
    mock_client = unittest.mock.MagicMock()
    mock_client_class.return_value = mock_client

    mock_insert = unittest.mock.MagicMock()
    mock_client.insert_rows_json = mock_insert
    mock_insert.return_value = None

    system_testing_utils.insert_rows_to_table([], 'table_id')
    mock_insert.assert_called_with('table_id', [])

  @unittest.mock.patch('google.cloud.bigquery.Client')
  def test_insert_rows_to_table_got_error(self, mock_client_class):
    mock_client = unittest.mock.MagicMock()
    mock_client_class.return_value = mock_client

    mock_insert = unittest.mock.MagicMock()
    mock_client.insert_rows_json = mock_insert
    mock_insert.return_value = 'error'

    with self.assertRaises(RuntimeError):
      system_testing_utils.insert_rows_to_table([], 'table_id')
