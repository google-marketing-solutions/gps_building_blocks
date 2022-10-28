# coding=utf-8
# Copyright 2021 Google LLC.
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
r"""Library for running the ML Windowing Pipeline.

ML Windowing Pipeline library to generate windowed features, outputing the
intermediate data and final features in the given BigQuery dataset. Note:
1. For concurrent runs, either use separate output datasets, or use separate
   run_ids so that the stages of the different pipeline do not interact.
2. There is no provision for retrying a failed stage in the pipeline. Either the
   script will crash, or the pipeline will continue with the next stage, even
   if the required intermediate data has not been generated.

Example Usage:

run_end_to_end_pipeline(params), where the param names and descriptions are:

project_id: Google Cloud project to run inside.
dataset_id: BigQuery dataset to write the output.
run_id: Optional suffix for the output tables. Must be compatible with BigQuery
        table naming requirements.

# BigQuery input, intermediate and output table flags.
analytics_table: Full BigQuery id of the Google Analytics/Firebase table.

# Windowing flags.
snapshot_start_date: YYYY-MM-DD date of the first window snapshot.
snapshot_end_date: YYYY-MM-DD date of the last window snapshot.
timezone: Google Analytics Data Timezone, e.g. "Australia/Sydney", or "+11:00".
    Default: UTC.
slide_interval_in_days: Number of days between successive windows.
lookback_window_gap_in_days: The lookback window ends on
    (snapshot_ts - lookback_window_gap_in_days) days. Sessions outside the
    lookback window are ignored.
lookback_window_size_in_days: The lookback window starts on
    (snapshot_ts - lookback_window_size_in_days - lookback_window_gap_in_days)
    days. Sessions outside the lookback window are ignored.
prediction_window_gap_in_days: The prediction window starts on
    (snapshot_ts + prediction_window_gap_in_days) days. Conversions
    outside the prediction window are ignored.
prediction_window_size_in_days: The prediction window ends on
    (snapshot_ts + prediction_window_size_in_days +
    prediction_window_gap_in_days) days. Conversions outside the prediction
    window are ignored.
stop_on_first_positive: Stop making a user's windows after their first positive
    label. Default: False.

# Location of SQL templates that can be overridden by the user.
conversions_sql: Name of the conversion extraction SQL file in templates/.
    Default: conversions_google_analytics.sql.
sessions_sql: Name of the session extraction SQL file in templates/.
    Default: sessions_google_analytics.sql.
windows_sql: Name of the windows extraction SQL file in templates/.
    Default: sliding_windows.sql.
features_sql: Name of the feature extraction SQL file in templates/. Override
    the default value with `features_from_input.sql` for user-provided Feature
    Option configurations. Default: automatic_features.sql.

# Feature options:
# Automatic feature extraction.
top_n_values_per_fact: Extract the top n values by count for each categorical
    fact to turn into features in automatic feature extraction (
    automatic_features.sql only). Default: 3.
# Alternative feature extraction using command line flags.
sum_values: Feature Options for Sum.
avg_values: Feature Options for Average.
avgbyday_values: Feature Options for Average by Day.
count_values: Feature Options for Count.
countdistinct_values: Feature Options for Count Distinct.
mode_values: Feature Options for Mode.
proportions_values: Feature Options for Proportion.
latest_values: Feature Options for Recent.
max_values: Feature Options for Max.
min_values: Feature Options for Min.
"""

import datetime
import logging
import os
import re
import sys
import time
from typing import Any, Dict, Optional

from google.cloud import bigquery
import jinja2
import pytz

from gps_building_blocks.cloud.utils import cloud_storage
from gps_building_blocks.ml.data_prep.ml_windowing_pipeline import fact
from gps_building_blocks.ml.data_prep.ml_windowing_pipeline.feature_utils import merge_feature_option_list
from gps_building_blocks.ml.data_prep.ml_windowing_pipeline.feature_utils import parse_feature_option

logging.basicConfig(
    format='%(levelname)s: %(message)s', level=logging.INFO, stream=sys.stdout)

_TABLE_NAME_TO_ID = {
    'conversions_table': 'conversions',
    'sessions_table': 'sessions',
    'numeric_facts_table': 'numeric_facts',
    'categorical_facts_table': 'categorical_facts',
    'instances_table': 'instances',
    'windows_table': 'windows',
    'categorical_fact_value_to_column_name_table':
        'categorical_fact_value_to_column_name',
    'features_table': 'features',
}

_jinja_env = None


def _set_jinja_env(params: Dict[str, Any]):
  """Sets up the Jinja environment.

  Args:
    params: Dict from ml_windowing_pipeline parameter names to values.
  """

  loaders = []
  if params['templates_dir']:
    loaders.append(
        jinja2.FileSystemLoader(os.path.normpath(params['templates_dir'])))
  loaders.append(
      jinja2.FileSystemLoader(
          os.path.join(os.path.dirname(__file__), 'templates')))
  jinja_env = jinja2.Environment(
      loader=jinja2.ChoiceLoader(loaders),
      keep_trailing_newline=True,
      lstrip_blocks=True,
      trim_blocks=True)
  params['jinja_env'] = jinja_env


def _get_table_id(project_id, dataset, table_name, run_id):
  return '{0}.{1}.{2}_{3}'.format(project_id, dataset, table_name, run_id)


def _get_output_table_ids(project_id, dataset, run_id):
  table_name_to_id = {}
  for (table_name, table_id) in _TABLE_NAME_TO_ID.items():
    table_name_to_id[table_name] = _get_table_id(project_id, dataset, table_id,
                                                 run_id)
  return table_name_to_id


def _run_sql(client: bigquery.client.Client, template_sql: str,
             params: Dict[str, Any]) -> bigquery.table.RowIterator:
  """Runs a SQL query.

  Args:
    client: BigQuery client.
    template_sql: The SQL query statement.
    params: SQL query parameters.

  Returns:
    RowIterator query object.
  """
  sql = params['jinja_env'].get_template(template_sql).render(params)
  if params['verbose']:
    # Including a print here for easier debugging and to show pipeline progress.
    logging.info(sql)

  query_job = client.query(sql)
  while not query_job.done():
    elapsed_seconds = time.time() - query_job.started.timestamp()
    logging.info('BigQuery job is [%s]. %s seconds elapsed... ',
                 str(query_job.state), '%.2f' % elapsed_seconds)
    # Adds a sleep as a safeguard to avoid floods of requests.
    time.sleep(1)
  logging.info('BigQuery job is [%s].', query_job.state)
  return query_job.result()


def _get_automatic_feature_params(client: bigquery.client.Client,
                                  params: Dict[str, Any]) -> Dict[str, Any]:
  """Prepares and returns feature options for automatic feature generation.

  Args:
    client: BigQuery client.
    params: Dict from ml_windowing_pipeline parameter names to values.

  Returns:
    Dict from feature option parameter name to value.
  """
  automatic_features_params = {}
  if not params['prediction_mode']:
    _run_sql(client, 'rank_categorical_fact_values_by_count.sql', params)
  # Extract top fact values.
  fact_name_to_value_and_column_suffix = {}
  for (fact_name, fact_value,
       column_name_suffix) in _run_sql(client, 'load_top_categorical_facts.sql',
                                       params):
    if fact_name not in fact_name_to_value_and_column_suffix:
      fact_name_to_value_and_column_suffix[fact_name] = []
    fact_name_to_value_and_column_suffix[fact_name].append(
        (fact_value, column_name_suffix))
  automatic_features_params['fact_name_to_value_and_column_suffix'] = (
      fact_name_to_value_and_column_suffix)
  return automatic_features_params


def _get_feature_options_params(params: Dict[str, Any]) -> Dict[str, Any]:
  """Parses input into FeatureOptions.

  Args:
    params: Dict from ml_windowing_pipeline parameter names to values.

  Returns:
    Dict from feature option parameter name to value.
  """
  feature_option_params = {}
  feature_option_params['sum_feature_options'] = parse_feature_option(
      params['sum_values'])
  feature_option_params['avg_feature_options'] = parse_feature_option(
      params['avg_values'])
  feature_option_params['avgbyday_feature_options'] = parse_feature_option(
      params['avgbyday_values'])
  feature_option_params['count_feature_options'] = parse_feature_option(
      params['count_values'])
  feature_option_params['countdistinct_feature_options'] = parse_feature_option(
      params['countdistinct_values'])
  feature_option_params['mode_feature_options'] = parse_feature_option(
      params['mode_values'])
  feature_option_params['proportions_feature_options'] = parse_feature_option(
      params['proportions_values'])
  feature_option_params['latest_feature_options'] = parse_feature_option(
      params['latest_values'])
  feature_option_params['max_feature_options'] = parse_feature_option(
      params['max_values'])
  feature_option_params['min_feature_options'] = parse_feature_option(
      params['min_values'])
  feature_option_params[
      'count_proportion_feature_options'] = merge_feature_option_list(
          feature_option_params['proportions_feature_options'],
          feature_option_params['count_feature_options'])
  return feature_option_params


def _get_value_to_column_suffix_mapping_params(
    client: bigquery.client.Client, params: Dict[str, Any]) -> Dict[str, Any]:
  """Extracts a mapping from fact name and value to column suffix.

  For each feature_option in params, extracts a mapping from the fact name and
  value to a unique BigQuery column suffix.

  Args:
    client: BigQuery client.
    params: Dict from ml_windowing_pipeline parameter names to values.

  Returns:
    Dict containing fact_name_to_value_and_column_suffix or empty if
    there are no feature_options.
  """
  new_params = {}
  fact_name_to_value_and_column_suffix = {}
  if params['count_proportion_feature_options']:
    for (fact_name, fact_value,
         column_name_suffix) in _run_sql(client, 'feature_column_name.sql',
                                         params):
      if fact_name not in fact_name_to_value_and_column_suffix:
        fact_name_to_value_and_column_suffix[fact_name] = []
      fact_name_to_value_and_column_suffix[fact_name].append(
          (fact_value, column_name_suffix))
  new_params['fact_name_to_value_and_column_suffix'] = (
      fact_name_to_value_and_column_suffix)
  return new_params


def update_params_with_defaults(params):
  """Update paramaters with default value."""
  params.setdefault('run_id', '')
  params.setdefault('timezone', 'UTC')
  params.setdefault('stop_on_first_positive', False)
  params.setdefault('conversions_sql', 'conversions_google_analytics.sql')
  params.setdefault('sessions_sql', 'sessions_google_analytics.sql')
  params.setdefault('windows_sql', 'sliding_windows.sql')
  params.setdefault('numeric_facts_sql', 'numeric_facts.sql')
  params.setdefault('categorical_facts_sql', 'categorical_facts.sql')
  params.setdefault('prediction_mode', False)
  params.setdefault('features_sql', 'automatic_features.sql')
  params.setdefault('top_n_values_per_fact', 3)
  params.setdefault('sum_values', '')
  params.setdefault('avg_values', '')
  params.setdefault('avgbyday_values', '')
  params.setdefault('count_values', '')
  params.setdefault('countdistinct_values', '')
  params.setdefault('mode_values', '')
  params.setdefault('proportions_values', '')
  params.setdefault('latest_values', '')
  params.setdefault('max_values', '')
  params.setdefault('min_values', '')
  params.update(
      _get_output_table_ids(params['project_id'], params['dataset_id'],
                            params['run_id']))
  params.setdefault('verbose', False)
  params.setdefault('prediction_window_conversions_to_label_sql',
                    'prediction_window_conversions_to_label_binary.sql')
  params.setdefault('templates_dir', '')
  _set_jinja_env(params)


def check_gcp_params(params: Dict[str, Any]):
  """Runs basic checks on the set of Google Cloud Platform params."""
  assert params['project_id']
  assert 'dataset_id' in params


def check_snapshot_date_params(params: Dict[str, Any]):
  """Runs basic checks on the snapshot date params."""
  assert 'snapshot_start_date' in params
  assert 'snapshot_end_date' in params
  assert params['snapshot_start_date'] <= params['snapshot_end_date']


def check_slide_interval_params(params: Dict[str, Any]):
  """Runs basic checks on the slide interval params."""
  assert 'slide_interval_in_days' in params
  assert params['slide_interval_in_days'] > 0


def check_lookback_window_params(params: Dict[str, Any]):
  """Runs basic checks on the lookback window params."""
  assert 'lookback_window_size_in_days' in params
  assert 'lookback_window_gap_in_days' in params
  assert params['lookback_window_size_in_days'] > 0
  assert params['lookback_window_gap_in_days'] >= 0


def check_prediction_window_params(params: Dict[str, Any]):
  """Runs basic checks on the prediction window params."""
  assert 'prediction_window_gap_in_days' in params
  assert 'prediction_window_size_in_days' in params
  assert params['prediction_window_size_in_days'] > 0
  assert params['prediction_window_gap_in_days'] >= 1


def generate_conversions_table(client: bigquery.client.Client,
                               params: Dict[str, Any]):
  """Generates the table of user conversion data."""
  assert 'conversions_table' in params
  _run_sql(client, params['conversions_sql'], params)


def generate_sessions_table(client: bigquery.client.Client,
                            params: Dict[str, Any]):
  """Generates the table of user session data."""
  _run_sql(client, params['sessions_sql'], params)


def update_fact_params(client: bigquery.client.Client, params: Dict[str, Any]):
  """Updates params with lists of facts, and numerical and categorical facts."""
  sessions_table = client.get_table(params['sessions_table'])
  params['facts'] = fact.Fact.extract_facts(sessions_table)
  params['numeric_facts'] = fact.Fact.get_numeric_facts(params['facts'])
  params['categorical_facts'] = fact.Fact.get_categorical_facts(params['facts'])


def generate_numeric_facts_table(client: bigquery.client.Client,
                                 params: Dict[str, Any]):
  """Generates the table of numerical facts."""
  _run_sql(client, params['numeric_facts_sql'], params)


def generate_categorical_facts_table(client: bigquery.client.Client,
                                     params: Dict[str, Any]):
  """Generates the table of categorical facts."""
  _run_sql(client, params['categorical_facts_sql'], params)


def generate_instances_table(client: bigquery.client.Client,
                             params: Dict[str, Any]):
  """Generates the table of instances."""
  _run_sql(client, 'instances.sql', params)


def generate_windows_table(client: bigquery.client.Client,
                           params: Dict[str, Any]):
  """Generates the table of windowed data."""
  _run_sql(client, params['windows_sql'], params)
  if params['stop_on_first_positive']:
    _run_sql(client, 'stop_on_first_positive.sql', params)


def generate_features_table(client: bigquery.client.Client,
                            params: Dict[str, Any]):
  """Generates the table of features."""
  if params['features_sql'] == 'automatic_features.sql':
    params.update(_get_automatic_feature_params(client, params))
  elif params['features_sql'] == 'features_from_input.sql':
    params.update(_get_feature_options_params(params))
    params.update(_get_value_to_column_suffix_mapping_params(client, params))
  _run_sql(client, params['features_sql'], params)


def run_end_to_end_pipeline(params: Dict[str, Any],
                            client: Optional[bigquery.Client] = None):
  """Runs the ML Windowing Pipeline with the given params end to end.

  Args:
    params: Dict from ml_windowing_pipeline parameter names to values.
    client: If provided, use this BigQuery Client. Otherwise, build a new
        Client with default credentials.
  """
  check_gcp_params(params)
  assert 'analytics_table' in params
  check_snapshot_date_params(params)
  check_slide_interval_params(params)
  check_lookback_window_params(params)
  check_prediction_window_params(params)
  update_params_with_defaults(params)

  if not client:
    client = bigquery.Client()

  generate_conversions_table(client, params)
  generate_sessions_table(client, params)
  update_fact_params(client, params)
  generate_numeric_facts_table(client, params)
  generate_categorical_facts_table(client, params)
  generate_instances_table(client, params)
  generate_windows_table(client, params)
  generate_features_table(client, params)


def run_data_extraction_pipeline(params: Dict[str, Any],
                                 client: Optional[bigquery.Client] = None):
  """Runs Pipeline 1: The Data Extraction Pipeline.

  Extracts conversion and session data from the specified analytics table. Use
  the sample conversion and session SQL files in templates/ to write your own
  custom conversion and session data extraction definitions, using params to
  overwrite the defaults provided.

  Args:
    params: Dict from pipeline parameter names to values.
    client: If provided, use this BigQuery Client. Otherwise, build a new
        Client with default credentials.
  """
  check_gcp_params(params)
  assert 'analytics_table' in params
  update_params_with_defaults(params)

  if not client:
    client = bigquery.Client()

  generate_conversions_table(client, params)
  generate_sessions_table(client, params)


def run_data_exploration_pipeline(params: Dict[str, Any],
                                  client: Optional[bigquery.Client] = None):
  """Runs Pipeline 2: The Data Exploration Pipeline.

  Extracts numeric and categorical facts into BigQuery tables for data
  exploration and analysis. This can help find anomolous data and facts that
  might decrease the performance of the machine learning algorithm. Also
  extracts instances, which can help in determining the best
  window size etc.

  Note that you must specify `prediction_window_conversions_to_label_sql`
  parameter if you are not using binary classification. Set it to
  `prediction_window_conversions_to_label_regression.sql` for Regression. For
  other methods (e.g multi-class), set it to the name of the template you have
  created (e.g. prediction_window_conversions_to_label_multi_class.sql).

  Args:
    params: Dict from pipeline parameter names to values.
    client: If provided, use this BigQuery Client. Otherwise, build a new
        Client with default credentials.
  """
  check_gcp_params(params)
  check_snapshot_date_params(params)
  check_slide_interval_params(params)
  check_prediction_window_params(params)
  update_params_with_defaults(params)

  if not client:
    client = bigquery.Client()

  update_fact_params(client, params)
  generate_numeric_facts_table(client, params)
  generate_categorical_facts_table(client, params)
  generate_instances_table(client, params)


def run_windowing_pipeline(params: Dict[str, Any],
                           client: Optional[bigquery.Client] = None):
  """Runs Pipeline 3: The Windowing Pipeline.

  Segments the user data into multiple, potentially overlapping, time windows,
  with each window containing a lookback window and a prediction window. By
  default, the sliding_windows.sql algorithm is used. Use the param windows_sql
  to replace this with a different algorithm, like session based windowing in
  session_windows.sql.

  Note that you must specify `prediction_window_conversions_to_label_sql`
  parameter if you are not using binary classification. Set it to
  `prediction_window_conversions_to_label_regression.sql` for Regression. For
  other methods (e.g multi-class), set it to the name of the template you have
  created (e.g. prediction_window_conversions_to_label_multi_class.sql).

  Args:
    params: Dict from pipeline parameter names to values.
    client: If provided, use this BigQuery Client. Otherwise, build a new
        Client with default credentials.
  """
  check_gcp_params(params)
  check_snapshot_date_params(params)
  check_lookback_window_params(params)
  check_prediction_window_params(params)
  update_params_with_defaults(params)

  if not client:
    client = bigquery.Client()

  update_fact_params(client, params)
  if params['windows_sql'] == 'sliding_windows.sql':
    assert 'slide_interval_in_days' in params
    assert params['slide_interval_in_days'] > 0
  generate_windows_table(client, params)


def run_features_pipeline(params: Dict[str, Any],
                          client: Optional[bigquery.Client] = None):
  """Runs Pipeline 4: The Feature Generation Pipeline.

  Generates features from the windows of data computed in Pipeline 3. By
  default, features are generated automatically. For more precise feature
  generation, use the features_sql param to point to the features_from_input.sql
  file, or point to your own custom feature generation SQL script.

  Args:
    params: Dict from pipeline parameter names to values.
    client: If provided, use this BigQuery Client. Otherwise, build a new
        Client with default credentials.
  """
  check_gcp_params(params)
  update_params_with_defaults(params)

  if not client:
    client = bigquery.Client()

  update_fact_params(client, params)
  generate_features_table(client, params)


def generate_features_sql_template(params: Dict[str, Any],
                                   client: Optional[bigquery.Client] = None
                                   ) -> str:
  """Returns a SQL template string equivalent to the input feature parameters.

  Feature generation using features_from_input.sql allows the user to specify
  features using params like sum_values, max_values, etc. These string params
  can be very long. This pipeline generates a SQL template string equivalent
  to the feature params specified.

  Args:
    params: Dict from pipeline parameter names to values.
    client: If provided, use this BigQuery Client. Otherwise, build a new
        Client with default credentials.

  Returns:
    SQL Jinja template string for generating features.
  """
  params.update({
      'templates_dir': None,
      'verbose': False,
      'features_sql': 'features_from_input.sql',
      # The template params must remain as params after substitution.
      'windows_table': '{{windows_table}}',
      'sessions_table': '{{sessions_table}}',
      'prediction_mode': '{{prediction_mode}}',
      'features_table': '{{features_table}}',
  })
  _set_jinja_env(params)

  if not client:
    client = bigquery.Client()

  params.update(_get_feature_options_params(params))
  params.update(_get_value_to_column_suffix_mapping_params(client, params))
  return params['jinja_env'].get_template(params['features_sql']).render(params)


def run_prediction_pipeline(params: Dict[str, Any],
                            client: Optional[bigquery.Client] = None):
  """Runs the prediction pipeline, generating features for a single window.

  Before running this pipeline, first run the end-to-end windowing pipeline, and
  then use the data to train an ML model. Once the model is deployed and you
  want predictions about live customers, run this script to generate features
  for the customers over a single window of data, and then input the features
  into the ML model to get it's predictions.

  Args:
    params: Dict from pipeline parameter names to values.
    client: If provided, use this BigQuery Client. Otherwise, build a new
        Client with default credentials.
  """
  params['prediction_mode'] = True
  params['slide_interval_in_days'] = 1
  params['windows_sql'] = 'sliding_windows.sql'

  if params.get('snapshot_date') and params.get('snapshot_date_offset_in_days'):
    raise ValueError(
        'Specify either snapshot_date or snapshot_date_offset_in_days.')
  elif params.get('snapshot_date'):
    params['snapshot_start_date'] = params['snapshot_date']
    params['snapshot_end_date'] = params['snapshot_date']
  elif params.get('snapshot_date_offset_in_days'):
    offset_days = int(params['snapshot_date_offset_in_days'])
    end_date = (
        datetime.datetime.now(pytz.timezone(params['timezone']))
        - datetime.timedelta(days=offset_days))
    params['snapshot_start_date'] = end_date.strftime('%Y-%m-%d')
    params['snapshot_end_date'] = end_date.strftime('%Y-%m-%d')
  else:
    raise ValueError('Set snapshot_date or snapshot_date_offset_in_days.')
  if not params.get('run_id'):
    params['run_id'] = datetime.datetime.strptime(
        params['snapshot_end_date'], '%Y-%m-%d').strftime('%Y%m%d')

  # Prediction window settings are used in generating prediction label which is
  # irrelevant in this pipeline so setting prediction_window_gap_in_days and
  # prediction_window_size_in_days to 1.
  params['prediction_window_gap_in_days'] = 1
  params['prediction_window_size_in_days'] = 1

  check_gcp_params(params)
  assert 'analytics_table' in params
  check_lookback_window_params(params)
  check_prediction_window_params(params)
  # Save the user-specified fact_value_map_table.
  fact_value_map_table = params.get(
      'categorical_fact_value_to_column_name_table', None)
  update_params_with_defaults(params)
  # Overwrite the default categorical_fact_value_to_column_name_table with the
  # param version, which, if used, should point to the output table from a
  # historical training run of the end_to_end_pipeline.
  if fact_value_map_table:
    params['categorical_fact_value_to_column_name_table'] = fact_value_map_table

  if not client:
    client = bigquery.Client()

  generate_conversions_table(client, params)
  generate_sessions_table(client, params)
  update_fact_params(client, params)
  generate_windows_table(client, params)
  generate_features_table(client, params)

  if params['cloud_storage_bucket'] and params['cloud_storage_file_path']:
    params['conversions_table'] = 'conversions_${RUN_ID}'
    params['windows_table'] = 'windows_${RUN_ID}'
    params['features_table'] = 'features_${RUN_ID}'
    params['sessions_table'] = 'sessions_${RUN_ID}'
    params['snapshot_start_date'] = '${SNAPSHOT_DATE}'
    _export_sql(params, 'conversions_google_analytics.sql',
                'sentinel_ga_conversions.sql')
    _export_sql(params, 'sessions_google_analytics.sql',
                'sentinel_ga_sessions.sql')
    _export_sql(params, 'sliding_windows.sql', 'sentinel_windows.sql')
    _export_sql(params, 'features_from_input.sql', 'sentinel_features.sql')
    _export_sql(params, 'batch_scoring.sql', 'sentinel_batch_scoring.sql')


def _export_sql(params: Dict[str, Any], template_sql: str, output_file: str):
  """Exports the prediction pipeline sql files to cloud storage.

  This will be primarily used for the Sentinel automation.

  Args:
    params: Dict from pipeline parameter names to values.
    template_sql: The sql template file name.
    output_file: The file name for the output file in cloud storage.
  """
  cs_utils = cloud_storage.CloudStorageUtils(project_id=params['project_id'])
  sql = params['jinja_env'].get_template(template_sql).render(params)
  sql = _refine_sql_for_sentinel(sql)
  cs_utils.write_to_file(sql, params['cloud_storage_bucket'],
                         params['cloud_storage_file_path'] + output_file)


def _refine_sql_for_sentinel(content: str) -> str:
  """Refines the sql content to make it work for Sentinel.

  Sentinel link:
  https://github.com/GoogleCloudPlatform/cloud-for-marketing/tree/main/marketing-analytics/activation/data-tasks-coordinator
  Removes CREATE OR REPLACE TABLE statements and just leave it with SELECT
  statements. This will be primarily used for the Sentinel automation.

  Args:
    content: SQL content.

  Returns:
    Refined SQL content.
  """
  return re.sub(r'CREATE OR REPLACE TABLE.*\n.*AS', '', content)
