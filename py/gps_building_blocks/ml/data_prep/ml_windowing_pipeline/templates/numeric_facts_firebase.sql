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
# SQL Jinja template to output all numeric facts for Firebase as input for data visualization.
# Args:
#   analytics_table: input Firebase Analytics BigQuery tablename.
#   numeric_facts_table: output sessions BigQuery tablename.
#
# Output schema:
#   session_id: STRING
#   user_id: STRING
#   ts: TIMESTAMP  (timestamp of the fact)
#   name: STRING   (name of the fact)
#   value: STRING  (value of the fact converted to a NUMERIC)

CREATE OR REPLACE TABLE `{{numeric_facts_table}}`
AS (
  SELECT
    CONCAT(user_pseudo_id, '/', event_date) AS session_id,
    user_pseudo_id AS user_id,
    TIMESTAMP_MICROS(event_timestamp) AS ts,
    CONCAT(event_name, '_numeric_', EventParams.key) AS name,
    CAST(
      COALESCE(
        EventParams.value.int_value,
        EventParams.value.double_value,
        EventParams.value.float_value) AS NUMERIC) AS value
  FROM `{{analytics_table}}` AS AnalyticTable,
    UNNEST(AnalyticTable.event_params) EventParams
  WHERE
    COALESCE(
      EventParams.value.int_value,
      EventParams.value.double_value,
      EventParams.value.float_value) IS NOT NULL

);
