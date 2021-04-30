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
# SQL Jinja template to output instances based on user activity over time.
# Args:
#   sessions_table: input sessions BigQuery tablename from the output of sessions.sql
#   snapshot_start_date: first date to create a snapshot summary (STRING with format 'YYYY-MM-DD')
#   snapshot_end_date: last date to create a snapshot summary (STRING with format 'YYYY-MM-DD')
#   timezone: Timezone for the Google Analytics Data, e.g. "Australia/Sydney", or "+11:00"
#   slide_interval_in_days: number of days between successive snapshot summary dates
#   conversions_table: input conversions BigQuery tablename from the output of conversions.sql
#   prediction_window_gap_in_days:
#       ignore conversions before this many days following the snapshot summary date
#   prediction_window_size_in_days:
#       ignore conversion after this many days following the snapshot summary date +
#       prediction_window_gap_in_days
#   prediction_window_conversions_to_label_sql: SQL file that converts an array of conversions into
#       a label. Default is `prediction_window_conversions_to_label_binary.sql`.
#   instances_table: output BigQuery tablename to write the instances.
#
# Each row in the output is an instance based on all the activity for one user up to the snapshot
# date, and also includes the array of any conversions from the following prediction window. Note
# that the snapshot_start_date is just the first date that an instance is produced. A user's first
# activity can occur before this date.

CREATE OR REPLACE TABLE `{{instances_table}}`
AS (
  WITH SnapshotDates AS (
    SELECT
      TIMESTAMP(snapshot_date, '{{timezone}}') AS snapshot_ts
    FROM
      UNNEST(GENERATE_DATE_ARRAY(
        '{{snapshot_start_date}}', '{{snapshot_end_date}}', INTERVAL {{slide_interval_in_days}} DAY
      )) AS snapshot_date
  ), UserSessionSnapshots AS (
    SELECT
      SnapshotDates.snapshot_ts,
      Sessions.user_id,
      TIMESTAMP_DIFF(
        SnapshotDates.snapshot_ts, TIMESTAMP('{{snapshot_start_date}}'), DAY
      ) AS days_since_start_date,
      TIMESTAMP_DIFF(SnapshotDates.snapshot_ts, MIN(session_ts), DAY) AS days_since_first_activity,
      TIMESTAMP_DIFF(SnapshotDates.snapshot_ts, MAX(session_ts), DAY) AS days_since_latest_activity,
    FROM `{{sessions_table}}` AS Sessions
    CROSS JOIN SnapshotDates
    WHERE Sessions.session_ts < SnapshotDates.snapshot_ts
    GROUP BY Sessions.user_id, SnapshotDates.snapshot_ts
  ), PredictionWindowConversions AS (
    SELECT
      ConversionTable.user_id,
      SnapshotDates.snapshot_ts,
      ARRAY_AGG(ConversionTable) AS conversions,
    FROM `{{conversions_table}}` AS ConversionTable
    CROSS JOIN SnapshotDates
    WHERE
      ConversionTable.conversion_ts BETWEEN
        TIMESTAMP_ADD(SnapshotDates.snapshot_ts, INTERVAL {{prediction_window_gap_in_days}} DAY)
        AND TIMESTAMP_ADD(
          SnapshotDates.snapshot_ts,
          INTERVAL ({{prediction_window_size_in_days}} + {{prediction_window_gap_in_days}}) DAY)
    GROUP BY ConversionTable.user_id, SnapshotDates.snapshot_ts
  )
  SELECT
    UserSessionSnapshots.*,
    {% include '%s' % prediction_window_conversions_to_label_sql %}
      # Column name on a newline in case the last line of the SQL injection is a comment
      AS label,
  FROM UserSessionSnapshots
  LEFT JOIN PredictionWindowConversions
    USING (user_id, snapshot_ts)
);

