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
# Sample SQL Jinja template to extract customer firebase conversions into an internal format.
# Args:
#   analytics_table: input Firebase Analytics BigQuery tablename containing the conversions.
#   conversions_table: output conversions BigQuery tablename.
#
# This is only a sample file. Override it with your own query that extracts firebase conversions.

CREATE OR REPLACE TABLE `{{conversions_table}}`
AS (
  SELECT DISTINCT
    COALESCE(user_id, user_pseudo_id) AS user_id,
    TIMESTAMP_MICROS(event_timestamp) AS conversion_ts,
    TRUE AS label
  FROM `{{analytics_table}}`
  WHERE
    event_name = 'in_app_purchase'
    AND (user_pseudo_id IS NOT NULL OR user_id IS NOT NULL)
);
