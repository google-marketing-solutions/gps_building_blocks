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

r"""Outputs a SQL template string, equivalent to the input feature parameters.

Feature generation using features_from_input.sql allows the user to specify
features using params like sum_values, max_values, etc. These string params
can be very long. This script generates a SQL template string equivalent to the
feature params specified.

Example Usage:

Automatic Feature Generation
python generate_features_sql_template.py \
--sum_values='totals_visits;totals_hits' \
--avg_values='totals_visits;totals_hits' \
--avgbyday_values='totals_visits;totals_hits' \
--count_values='geoNetwork_metro:[Providence-New Bedford///,MA",Rochester-Mason City-Austin///,IA]:[Others]' \
--countdistinct_values='hits_eventInfo_eventAction' \
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

# Feature extraction using command line flags.
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


def main(_):
  params = FLAGS.flag_values_dict()
  params['features_sql'] = 'features_from_input.sql'
  sql = ml_windowing_pipeline.generate_features_sql_template(params)
  print(sql)
  return 0


if __name__ == '__main__':
  app.run(main)
