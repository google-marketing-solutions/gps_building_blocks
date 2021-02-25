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
# Sample SQL Jinja template to extract customer conversions into an internal format.
# Args:
#   analytics_table: input Google Analytics BigQuery tablename containing the conversions.
#   conversions_table: output conversions BigQuery tablename.
#
# This is only a sample file. Override it with your own query that extracts conversions. Note that
# the input data is not limited to conversions contained in GA. You can rewrite this query to
# extract data from Firebase, or even your own CRM data. The only requirement is that the output of
# the query matches the following schema:
#
# Output schema:
#   user_id: STRING
#   conversion_ts: TIMESTAMP (timestamp of the conversion)
#   label: ANY type
#     e.g. BOOL for binary classification, STRING for multi-class, or INT64/FLOAT64 for regression.

CREATE OR REPLACE TABLE `{{conversions_table}}`
AS (
  SELECT DISTINCT
    GaTable.fullVisitorId AS user_id,
    TIMESTAMP_SECONDS(GaTable.visitStartTime) AS conversion_ts,
    TRUE AS label
  FROM
    `{{analytics_table}}` AS GaTable, UNNEST(GaTable.hits) as hits
  WHERE
    hits.eCommerceAction.action_type = '6'  -- Google Analytics code for "Completed purchase"
);
