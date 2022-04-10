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

r"""Script to run Pipeline 4: The Feature Generation Pipeline.

Generates features from the windows of data computed in Pipeline 3. By default,
features are generated automatically. For more precise feature generation,
use the --features_sql flag to point to the features_from_input.sql file (see
the second example usage below), or point to your own custom feature generation
SQL script.

Example Usage:

Automatic Feature Generation
python run_features_pipeline.py \
--project_id=<PROJECT_ID> \
--dataset_id=<DATASET_ID> \
--run_id=<OPTIONAL RUN_ID> \
--features_sql=<OPTIONAL alternative feature extraction SQL script file>

Manual Feature Generation
python run_features_pipeline.py \
--project_id=<PROJECT_ID> \
--dataset_id=<DATASET_ID> \
--run_id=<OPTIONAL RUN_ID> \
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
                    'with BigQuery table naming requirements. Note the same '
                    'run_id must be used for all pipelines in the same run.')
# Feature options:
flags.DEFINE_string('features_sql', 'automatic_features.sql',
                    'Name of the feature extraction SQL file in templates/.'
                    'Override default value with `features_from_input.sql` for '
                    'user-provided Feature Option configurations.')
flags.DEFINE_string('templates_dir', '', 'Alternative templates directory.')
# Automatic feature extraction.
flags.DEFINE_integer('top_n_values_per_fact', 3,
                     'Extract the top n values by count for each '
                     'categorical fact to turn into features in automatic '
                     'feature extraction (automatic_features.sql only).',
                     lower_bound=1)
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
  ml_windowing_pipeline.run_features_pipeline(params)
  return 0


if __name__ == '__main__':
  app.run(main)
