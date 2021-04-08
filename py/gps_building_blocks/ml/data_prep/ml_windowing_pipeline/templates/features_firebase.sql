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
# Sample SQL Jinja template to generate Firebase Analytics features from windows of facts.
# Args:
#   windows_table: input BigQuery table name containing windows, e.g. from sliding_windows.sql
#   prediction_mode: True for no label output, and False otherwise.
#   features_table: output BigQuery table name to write the features.
#
# We recommend using automatic_feature_generation.sql instead. If you want to manually specify
# features, use this file as a guide and overwrite it with the features you want to extract. Several
# example features are included below for reference, though you are not limited to these.
#
# Output schema:
#   window_start_ts: TIMESTAMP,
#   window_end_ts: TIMESTAMP,
#   snapshot_ts: TIMESTAMP
#   user_id: STRING
#   label: type defined in prediction_window_conversions_to_label.sql
#   <FeatureName1>: <FeatureType1>
#   <FeatureName2>: <FeatureType2>
#   ...

CREATE OR REPLACE TABLE `{{features_table}}` AS (
  SELECT
    Windows.window_start_ts,
    Windows.window_end_ts,
    Windows.snapshot_ts,
    Windows.user_id,
    {% if not prediction_mode %}
    Windows.label,
    {% endif %}

    # Out-of-window activity features.
    (
      SELECT TIMESTAMP_DIFF(Windows.snapshot_ts, MIN(Sessions.session_ts), DAY)
      FROM `{{sessions_table}}` AS Sessions
      WHERE
        Sessions.user_id = Windows.user_id
        AND Sessions.session_ts < Windows.snapshot_ts
    ) AS days_since_first_activity,
    (
      SELECT TIMESTAMP_DIFF(Windows.snapshot_ts, MAX(Sessions.session_ts), DAY)
      FROM `{{sessions_table}}` AS Sessions
      WHERE
        Sessions.user_id = Windows.user_id
        AND Sessions.session_ts < Windows.snapshot_ts
    ) AS days_since_latest_activity,

    # Seasonality / Time based features
    CONCAT(EXTRACT(DAYOFWEEK FROM Windows.snapshot_ts), 'D') AS snapshot_ts_day_of_week,
    CONCAT(EXTRACT(ISOWEEK FROM Windows.snapshot_ts), 'W') AS snapshot_ts_week_of_year,
    CONCAT(EXTRACT(MONTH FROM Windows.snapshot_ts), 'M') AS snapshot_ts_month_of_year,

    # Features for numerical facts
    (SELECT IFNULL(SUM(Fact.value), 0) FROM UNNEST(Windows.event_param_score) AS Fact)
      AS sum_event_param_score,
    (SELECT IFNULL(AVG(Fact.value), 0) FROM UNNEST(Windows.event_param_score) AS Fact)
      AS avg_event_param_score,

    # Features for categorical facts
    (SELECT Fact.value FROM UNNEST(Windows.event_name) AS Fact ORDER BY ts DESC LIMIT 1)
      AS latest_event_name,
    SAFE_DIVIDE(
      (
        SELECT COUNT(Fact.value)
        FROM UNNEST(Windows.trafficSource_source) AS Fact
        WHERE Fact.value = 'google-play'
      ),
      (SELECT COUNT(*) FROM UNNEST(Windows.trafficSource_source))
    ) AS proportion_trafficSource_source_google_play,
    SAFE_DIVIDE(
      (
        SELECT COUNT(Fact.value)
        FROM UNNEST(Windows.trafficSource_source) AS Fact
        WHERE Fact.value = '(direct)'
      ),
      (SELECT COUNT(*) FROM UNNEST(Windows.trafficSource_source))
    ) AS proportion_trafficSource_source_direct,

  FROM
    `{{windows_table}}` AS Windows
);

