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

Before running this pipeline, first run the end-to-end windowing pipeline using
sliding windows, and then use the data to train an ML model. Once the model is
deployed and you want predictions about live customers, run this script to
generate features for the customers over a single window of data, and then input
the features into the ML model to get it's predictions.

Example Usage:

python run_prediction_pipeline.py \
--project_id=<PROJECT_ID> \
--dataset_id=<DATASET_ID> \
--analytics_table="bigquery-public-data.google_analytics_sample.ga_sessions_*" \
--snapshot_date_offset_in_days=1 \
--lookback_window_size_in_days=30 \
--lookback_window_gap_in_days=0 \
--categorical_fact_value_to_column_name_table=<BIGQUERY TABLE from training run>

python run_prediction_pipeline.py \
--project_id=<PROJECT_ID> \
--dataset_id=<DATASET_ID> \
--analytics_table="bigquery-public-data.google_analytics_sample.ga_sessions_*" \
--snapshot_date_offset_in_days=1 \
--lookback_window_size_in_days=30 \
--lookback_window_gap_in_days=0 \
--features_sql='features_from_input.sql' \
--sum_values='totals_visits;totals_hits' \
--avg_values='totals_visits;totals_hits' \
--avgbyday_values='totals_visits;totals_hits' \
--count_values='geoNetwork_metro:[Providence-New Bedford///,MA",Rochester-Mason City-Austin///,IA]:[Others]' \
--countdistinct_values='hits_eventInfo_eventAction' \
--mode_values='hits_eCommerceAction_action_type:[3]:[Others]' \
--proportions_values='channelGrouping:[Organic Search,Social,Direct,Referral,Paid Search,Affiliates]:[Others]' \
--latest_values='device_isMobile:[false,true]:[Others]'
"""

from absl import app
from absl import flags

from gps_building_blocks.ml.data_prep.ml_windowing_pipeline import ml_windowing_pipeline

FLAGS = flags.FLAGS

# GCP flags.
flags.DEFINE_string('project_id', None, 'Google Cloud project to run inside.')
flags.DEFINE_string('dataset_id', None, 'BigQuery dataset to write the output.')
flags.DEFINE_string('run_id', None,
                    'By default, all output tables have the snapshot_date '
                    'suffix. Use this flag to override the suffix which must '
                    'be compatible with BigQuery table naming requirements.')
# BigQuery input flags.
flags.DEFINE_string('analytics_table', None,
                    'Full BigQuery id of the Google Analytics/Firebase table.')
# Windowing flags.
# Either set the snapshot_date with a specific date or the
# snapshot_date_offset_in_days with a relative offset from today.
flags.DEFINE_string('snapshot_date', None,
                    'YYYY-MM-DD snapshot date to make predictions from.')
flags.DEFINE_integer('snapshot_date_offset_in_days', None,
                     'Number of days before today to set the snapshot_date.')
flags.DEFINE_string('timezone', 'UTC',
                    'Timezone for the Google Analytics Data, '
                    'e.g. "Australia/Sydney", or "+11:00"')
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

# Location of SQL templates that can be overridden by the user.
flags.DEFINE_string('conversions_sql', 'conversions_google_analytics.sql',
                    'Name of the conversion extraction SQL file in templates/.')
flags.DEFINE_string('sessions_sql', 'sessions_google_analytics.sql',
                    'Name of the session extraction SQL file in templates/.')
flags.DEFINE_string('features_sql', 'automatic_features.sql',
                    'Name of the feature extraction SQL file in templates/.'
                    'Override default value with `features_from_input.sql` for '
                    'user-provided Feature Option configurations.')
flags.DEFINE_string('templates_dir', '', 'Alternative templates directory.')
# Feature options:
# Required flag only when using automatic_features.sql.
flags.DEFINE_string('categorical_fact_value_to_column_name_table', None,
                    'BigQuery table containing the fact (name, value) to '
                    'column name mapping from the model\'s training run using '
                    'automatic_features.sql.')
# Alternative feature extraction using command line flags.
flags.DEFINE_string('sum_values', '', 'Feature Options for Sum')
flags.DEFINE_string('avg_values', '', 'Feature Options for Average')
flags.DEFINE_string('avgbyday_values', '', 'Feature Options for Average by Day')
flags.DEFINE_string('count_values', '', 'Feature Options for Count')
flags.DEFINE_string('countdistinct_values', '',
                    'Feature Options for Count Distinct')
flags.DEFINE_string('mode_values', '', 'Feature Options for Mode')
flags.DEFINE_string('proportions_values', '', 'Feature Options for Proportion')
flags.DEFINE_string('latest_values', '', 'Feature Options for Recent')
flags.DEFINE_string('max_values', '', 'Feature Options for Max')
flags.DEFINE_string('min_values', '', 'Feature Options for Min')
# Debug flag.
flags.DEFINE_bool('verbose', False, 'Debug logging.')


def main(_):
  params = FLAGS.flag_values_dict()
  ml_windowing_pipeline.run_prediction_pipeline(params)
  return 0


if __name__ == '__main__':
  app.run(main)
