# python3
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

r"""Script to run the end-to-end ML Windowing Pipeline.

Simple script to generate windowed features, outputing the intermediate data
and final features in the given BigQuery dataset. This is equivalent to running
the following pipelines in sequence:
1. run_data_extraction_pipeline
2. run_data_exploration_pipeline
3. run_windowing_pipeline
4. run_features_pipeline

Example Usage:

python run_end_to_end_pipeline.py \
--project_id=<PROJECT_ID> \
--dataset_id=<DATASET_ID> \
--run_id=<OPTIONAL RUN_ID> \
--analytics_table="bigquery-public-data.google_analytics_sample.ga_sessions_*" \
--snapshot_start_date="2016-11-17" \
--snapshot_end_date="2017-07-01" \
--slide_interval_in_days=7 \
--lookback_window_size_in_days=30 \
--lookback_window_gap_in_days=0 \
--prediction_window_gap_in_days=1 \
--prediction_window_size_in_days=14 \
--stop_on_first_positive=True \
--top_n_values_per_fact=3

python run_end_to_end_pipeline.py \
--project_id=<PROJECT_ID> \
--dataset_id=<DATASET_ID> \
--run_id=<OPTIONAL RUN_ID> \
--analytics_table="bigquery-public-data.google_analytics_sample.ga_sessions_*" \
--snapshot_start_date="2016-11-17" \
--snapshot_end_date="2017-07-01" \
--slide_interval_in_days=7 \
--lookback_window_size_in_days=30 \
--lookback_window_gap_in_days=0 \
--prediction_window_gap_in_days=1 \
--prediction_window_size_in_days=14 \
--stop_on_first_positive=True \
--features_sql='features_from_input.sql' \
--sum_values='totals_visits;totals_hits' \
--avg_values='totals_visits;totals_hits' \
--count_values='geoNetwork_metro:[Providence-New Bedford///,MA",Rochester-Mason City-Austin///,IA]:[Others]' \
--mode_values='hits_eCommerceAction_action_type:[3]:[Others]' \
--proportions_values='channelGrouping:[Organic Search,Social,Direct,Referral,Paid Search,Affiliates]:[Others]' \
--latest_values='device_isMobile:[false,true]:[Others]' \
--max_values='totals_visits;totals_hits' \
--min_values='totals_visits;totals_hits'
"""

from absl import app
from absl import flags
from gps_building_blocks.ml.data_prep.ml_windowing_pipeline import ml_windowing_pipeline

FLAGS = flags.FLAGS

# GCP flags.
flags.DEFINE_string('project_id', None, 'Google Cloud project to run inside.')
flags.DEFINE_string('dataset_id', None, 'BigQuery dataset to write the output.')
flags.DEFINE_string('run_id', '',
                    'Optional suffix for the output tables. Must be compatible '
                    'with BigQuery table naming requirements.')
# BigQuery input flags.
flags.DEFINE_string('analytics_table', None,
                    'Full BigQuery id of the Google Analytics/Firebase table.')
# Windowing flags.
flags.DEFINE_string('snapshot_start_date', None,
                    'YYYY-MM-DD date of the first window snapshot.')
flags.DEFINE_string('snapshot_end_date', None,
                    'YYYY-MM-DD date of the last window snapshot.')
flags.DEFINE_string('timezone', 'UTC',
                    'Timezone for the Google Analytics Data, '
                    'e.g. "Australia/Sydney", or "+11:00"')
flags.DEFINE_integer('slide_interval_in_days', None,
                     'Number of days between successive windows.')
flags.DEFINE_integer('lookback_window_gap_in_days', None,
                     'The lookback window ends on (snapshot_ts - '
                     'lookback_window_gap_in_days) days. Sessions '
                     'outside the lookback window are ignored.',
                     lower_bound=0)
flags.DEFINE_integer('lookback_window_size_in_days', None,
                     'The lookback window starts on (snapshot_ts - '
                     'lookback_window_size_in_days - '
                     'lookback_window_gap_in_days) days. Sessions outside the '
                     'lookback window are ignored.',
                     lower_bound=0)
flags.DEFINE_integer('prediction_window_gap_in_days', None,
                     'The prediction window starts on (snapshot_ts + '
                     'prediction_window_gap_in_days) days. '
                     'Conversions outside the prediction window are ignored.',
                     lower_bound=1)
flags.DEFINE_integer('prediction_window_size_in_days', None,
                     'The prediction window ends on (snapshot_ts + '
                     'prediction_window_size_in_days + '
                     'prediction_window_gap_in_days) days. Conversions outside'
                     'the prediction window are ignored.',
                     lower_bound=1)
flags.DEFINE_bool('stop_on_first_positive', False,
                  'Stop making a user\'s windows after a first positive label.')


# Location of SQL templates that can be overridden by the user.
flags.DEFINE_string('conversions_sql', 'conversions_google_analytics.sql',
                    'Name of the conversion extraction SQL file in templates/.')
flags.DEFINE_string('sessions_sql', 'sessions_google_analytics.sql',
                    'Name of the session extraction SQL file in templates/.')
flags.DEFINE_string('windows_sql', 'sliding_windows.sql',
                    'Name of the windows extraction SQL file in templates/.')
flags.DEFINE_string('features_sql', 'automatic_features.sql',
                    'Name of the feature extraction SQL file in templates/.'
                    'Override default value with `features_from_input.sql` for '
                    'user-provided Feature Option configurations.')
flags.DEFINE_string('prediction_window_conversions_to_label_sql',
                    'prediction_window_conversions_to_label_binary.sql',
                    'Name of the mapping label to prediction window SQL file '
                    'in templates/.')
flags.DEFINE_string('templates_dir', '', 'Alternative templates directory.')
# Feature options:
# Automatic feature extraction.
flags.DEFINE_integer('top_n_values_per_fact', 3,
                     'Extract the top n values by count for each '
                     'categorical fact to turn into features in automatic '
                     'feature extraction (automatic_features.sql only).',
                     lower_bound=1)
# Alternative feature extraction using command line flags.
flags.DEFINE_string('sum_values', '', 'Feature Options for Sum')
flags.DEFINE_string('avg_values', '', 'Feature Options for Average')
flags.DEFINE_string('count_values', '', 'Feature Options for Count')
flags.DEFINE_string('mode_values', '', 'Feature Options for Mode')
flags.DEFINE_string('proportions_values', '', 'Feature Options for Proportion')
flags.DEFINE_string('latest_values', '', 'Feature Options for Recent')
flags.DEFINE_string('max_values', '', 'Feature Options for Max')
flags.DEFINE_string('min_values', '', 'Feature Options for Min')
# Debug flag.
flags.DEFINE_bool('verbose', False, 'Debug logging.')


def main(_):
  params = FLAGS.flag_values_dict()
  ml_windowing_pipeline.run_end_to_end_pipeline(params)
  return 0


if __name__ == '__main__':
  app.run(main)
