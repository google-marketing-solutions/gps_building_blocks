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
# SQL Jinja template to remove a user's windows after the first positive label.
# Args:
#   windows_table: BigQuery table name to read, filter and then write back windows
#
# Override this query to change the definition of a positive label. By default, labels are cast to
# INT64s, so a positive label is any label greater than 0.

CREATE OR REPLACE TABLE {{windows_table}} AS (
  WITH FirstPositiveLabel AS (
    SELECT
      user_id,
      MIN(snapshot_ts) AS first_positive_label_snapshot_ts,
    FROM {{windows_table}}
    WHERE
      SAFE_CAST(label AS INT64) > 0
    GROUP BY user_id
  )
  SELECT
    Windows.*
  FROM
    {{windows_table}} AS Windows
  LEFT JOIN FirstPositiveLabel
    ON Windows.user_id = FirstPositiveLabel.user_id
  WHERE
    FirstPositiveLabel.first_positive_label_snapshot_ts IS NULL
    OR Windows.snapshot_ts <= FirstPositiveLabel.first_positive_label_snapshot_ts
);
