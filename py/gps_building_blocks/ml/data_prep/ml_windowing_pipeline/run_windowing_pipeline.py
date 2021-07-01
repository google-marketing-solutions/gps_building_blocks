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

r"""Script to run Pipeline 3: The Windowing Pipeline.

Segments the user data into multiple, potentially overlapping, time windows,
with each window containing a lookback window and a prediction window. By
default, the sliding_windows.sql algorithm is used. Use the flag --windows_sql
to replace this with a different algorithm, like session based windowing in
session_windows.sql.

Note that you must specify `prediction_window_conversions_to_label_sql`
parameter if you are not using binary classification. Set it to
`prediction_window_conversions_to_label_regression.sql` for Regression. For
other methods (e.g multi-class), set it to the name of the template you have
created (e.g. prediction_window_conversions_to_label_multi_class.sql).

Example Usage:

python run_windowing_pipeline.py \
--project_id=<PROJECT_ID> \
--dataset_id=<DATASET_ID> \
--run_id=<OPTIONAL RUN_ID> \
--snapshot_start_date="2016-11-17" \
--snapshot_end_date="2017-07-01" \
--slide_interval_in_days=7 \
--lookback_window_size_in_days=30 \
--lookback_window_gap_in_days=0 \
--prediction_window_gap_in_days=1 \
--prediction_window_size_in_days=14 \
--stop_on_first_positive=True \
--windows_sql=<OPTIONAL alternative windowing SQL script file>
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
# Windowing flags.
flags.DEFINE_string('snapshot_start_date', None,
                    'YYYY-MM-DD date of the first window snapshot.')
flags.DEFINE_string('snapshot_end_date', None,
                    'YYYY-MM-DD date of the last window snapshot.')
flags.DEFINE_string('timezone', 'UTC',
                    'Timezone for the Google Analytics Data, '
                    'e.g. "Australia/Sydney", or "+11:00"')
flags.DEFINE_integer('slide_interval_in_days', None,
                     'Days between successive windows for sliding_windows.sql.')
flags.DEFINE_integer('lookback_window_gap_in_days', None,
                     'The lookback window ends on (snapshot_ts - '
                     'lookback_window_gap_in_days) days. Sessions '
                     'outside the lookback window are ignored.',
                     lower_bound=0)
flags.DEFINE_integer('lookback_window_size_in_days', None,
                     'The lookback window starts on (snapshot_ts - '
                     'lookback_window_size_in_days - '
                     'lookback_window_gap_in_days) days. Sessions outside the'
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
flags.DEFINE_string('windows_sql', 'sliding_windows.sql',
                    'Name of the windows extraction SQL file in templates/.')
flags.DEFINE_string('prediction_window_conversions_to_label_sql',
                    'prediction_window_conversions_to_label_binary.sql',
                    'Name of the mapping label to prediction window SQL file '
                    'in templates/.')
flags.DEFINE_string('templates_dir', '', 'Alternative templates directory.')
# Debug flag.
flags.DEFINE_bool('verbose', False, 'Debug logging.')


def main(_):
  params = FLAGS.flag_values_dict()
  ml_windowing_pipeline.run_windowing_pipeline(params)
  return 0


if __name__ == '__main__':
  app.run(main)
