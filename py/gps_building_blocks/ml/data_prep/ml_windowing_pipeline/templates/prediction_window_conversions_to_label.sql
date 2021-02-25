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
# SQL script fragment to convert an array of conversions into a label.
#
# This is only a sample file. Override it with your own query, following the format of the examples
# given below. Conversions are stored in PredictionWindowConversions as an array called conversions,
# sorted by timestamp.
#
# Binary classification:
IFNULL(
  (
    SELECT LOGICAL_OR(Conversions.label)
    FROM UNNEST(PredictionWindowConversions.conversions) AS Conversions
  ), FALSE)
#
# Regression
# IFNULL(
#  (
#    SELECT SUM(Conversions.label)
#    FROM UNNEST(PredictionWindowConversions.conversions) AS Conversions
#  ), 0)
#
# Regression with first conversion only: example 1
# IFNULL(PredictionWindowConversions.conversions[SAFE_OFFSET(0)].label, 0) AS label
#
# Regression with first conversion only: example 2
# IFNULL(
#  (
#    SELECT SUM(SAFE_CAST(Conversions.label AS INT64))
#    FROM UNNEST(PredictionWindowConversions.conversions) AS Conversions
#    ORDER BY Conversions.conversion_ts LIMIT 1)
#  , 0)
