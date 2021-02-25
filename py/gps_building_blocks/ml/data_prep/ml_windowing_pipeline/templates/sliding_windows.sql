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
# SQL Jinja template to output sliding windows over the user session and conversion data.
# Args:
#   snapshot_start_date: first date to create a window (STRING with format 'YYYY-MM-DD')
#   snapshot_end_date: last date to create a window (STRING with format 'YYYY-MM-DD')
#   timezone: Timezone for the Google Analytics Data, e.g. "Australia/Sydney", or "+11:00"
#   slide_interval_in_days: number of days between successive snapshot summary dates
#   facts: list of Facts (name, type) to include in a lookback window
#   lookback_window_gap_in_days: The lookback window ends on (snapshot_ts -
#       lookback_window_gap_in_days) days. Sessions outside the lookback window are ignored.
#   lookback_window_size_in_days: The lookback window starts on (snapshot_ts -
#       lookback_window_size_in_days - lookback_window_gap_in_days) days. Sessions outside the
#       lookback window are ignored.
#   conversions_table: input conversions BigQuery tablename from the output of conversions.sql
#   prediction_window_gap_in_days: The prediction window starts on (snapshot_ts +
#       prediction_window_gap_in_days) days. Conversions outside the prediction window
#       are ignored.
#   prediction_window_size_in_days: The prediction window ends on (snapshot_ts +
#       prediction_window_size_in_days + prediction_window_gap_in_days) days. Conversions outside
#       the prediction window are ignored.
#   prediction_window_conversions_to_label.sql: SQL file that converts an array of conversions into
#       a label. This file is included automatically. However, it needs to be edited to include
#       the specific logic for converting conversions into labels.
#   windows_table: output BigQuery tablename to write the sliding windows

CREATE OR REPLACE TABLE `{{windows_table}}`
AS (
  WITH SnapshotDates AS (
    SELECT
      TIMESTAMP(snapshot_date, '{{timezone}}') AS snapshot_ts
    FROM
      UNNEST(GENERATE_DATE_ARRAY(
        '{{snapshot_start_date}}', '{{snapshot_end_date}}', INTERVAL {{slide_interval_in_days}} DAY
      )) AS snapshot_date
  ), Windows AS (
    SELECT
      Sessions.user_id,
      SnapshotDates.snapshot_ts,
      TIMESTAMP_SUB(
        SnapshotDates.snapshot_ts,
        INTERVAL ({{lookback_window_size_in_days}} + {{lookback_window_gap_in_days}}) DAY)
        AS window_start_ts,
      TIMESTAMP_SUB(SnapshotDates.snapshot_ts, INTERVAL {{lookback_window_gap_in_days}} DAY)
        AS window_end_ts,
      {% for fact in facts %}
      ARRAY_CONCAT_AGG({{fact.name}}) AS {{fact.name}},
      {% endfor %}
    FROM `{{sessions_table}}` AS Sessions
    CROSS JOIN SnapshotDates
    WHERE
      session_ts BETWEEN
        TIMESTAMP_SUB(
          SnapshotDates.snapshot_ts,
          INTERVAL ({{lookback_window_size_in_days}} + {{lookback_window_gap_in_days}}) DAY)
        AND TIMESTAMP_SUB(SnapshotDates.snapshot_ts, INTERVAL {{lookback_window_gap_in_days}} DAY)
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
            INTERVAL ({{prediction_window_gap_in_days}} + {{prediction_window_size_in_days}}) DAY)
    GROUP BY ConversionTable.user_id, SnapshotDates.snapshot_ts
  )
  SELECT
    Windows.*,
    {% include 'prediction_window_conversions_to_label.sql' %}
    # Column name on a newline in case the last line of the SQL injection is a comment
      AS label,
    FROM Windows
    LEFT JOIN PredictionWindowConversions
      USING (user_id, snapshot_ts)
);
