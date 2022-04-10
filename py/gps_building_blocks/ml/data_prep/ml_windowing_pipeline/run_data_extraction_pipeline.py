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

r"""Script to run Pipeline 1: The Data Extraction Pipeline.

Extracts conversion and session data from the specified analytics table. Use the
sample conversion and session SQL files in templates/ to write your own custom
conversion and session data extraction definitions.

Example Usage:

python run_data_extraction_pipeline.py \
--project_id=<PROJECT_ID> \
--dataset_id=<DATASET_ID> \
--run_id=<OPTIONAL RUN_ID> \
--analytics_table="bigquery-public-data.google_analytics_sample.ga_sessions_*" \
--conversions_sql=<OPTIONAL alternative conversion extraction SQL script file> \
--sessions_sql=<OPTIONAL alternative session data extraction SQL script file>
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
# BigQuery input flags.
flags.DEFINE_string('analytics_table', None,
                    'Full BigQuery id of the Google Analytics/Firebase table.')
# Location of SQL templates that can be overridden by the user.
flags.DEFINE_string('conversions_sql', 'conversions_google_analytics.sql',
                    'Name of the conversion extraction SQL file in templates/.')
flags.DEFINE_string('sessions_sql', 'sessions_google_analytics.sql',
                    'Name of the session extraction SQL file in templates/.')
flags.DEFINE_string('templates_dir', '', 'Alternative templates directory.')
# Debug flag.
flags.DEFINE_bool('verbose', False, 'Debug logging.')


def main(_):
  params = FLAGS.flag_values_dict()
  ml_windowing_pipeline.run_data_extraction_pipeline(params)
  return 0


if __name__ == '__main__':
  app.run(main)
