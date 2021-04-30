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
#
# SQL script fragment to convert an array of conversions into label for binary classification.
#
# This is only a sample file for binary classification. Override it with your own query, following
# the format of the examples given below. Conversions are stored in PredictionWindowConversions as
# an array called conversions, sorted by timestamp.
#
# For other methods aside from binary classification and regression, create a new template file for
# it and it should contain SQL script fragment to convert an array of conversions into a label. Then
# set `prediction_window_conversions_to_label_sql` param to the name of the file you have created.
# Example: prediction_window_conversions_to_label_multi_class.sql

IFNULL(
  (
    SELECT LOGICAL_OR(Conversions.label)
    FROM UNNEST(PredictionWindowConversions.conversions) AS Conversions
  ), FALSE)
