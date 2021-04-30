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
# Sample SQL Jinja template to generate features from windows of facts.
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
#   snapshot_ts: TIMESTAMP,
#   user_id: STRING
#   label: type defined in `prediction_window_conversions_to_label_sql`
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
    (SELECT IFNULL(SUM(Fact.value), 0) FROM UNNEST(Windows.totals_visits) AS Fact)
      AS sum_totals_hits,
    (SELECT IFNULL(SUM(Fact.value), 0) FROM UNNEST(Windows.totals_timeOnScreen) AS Fact)
      AS sum_totals_timeOnScreen,
    (SELECT IFNULL(SUM(Fact.value), 0) FROM UNNEST(Windows.totals_totalTransactionRevenue) AS Fact)
      AS sum_totals_totalTransactionRevenue,

    (SELECT IFNULL(AVG(Fact.value), 0) FROM UNNEST(Windows.totals_timeOnScreen) AS Fact)
      AS avg_totals_timeOnScreen,
    (SELECT AVG(Fact.value) FROM UNNEST(Windows.hits_latencyTracking_pageDownloadTime) AS Fact)
      AS avg_hits_latencyTracking_pageDownloadTime,
    (SELECT AVG(Fact.value) FROM UNNEST(Windows.hits_latencyTracking_pageLoadTime) AS Fact)
      AS avg_hits_latencyTracking_pageLoadTime,

    (SELECT MAX(value) FROM UNNEST(Windows.hits_latencyTracking_pageDownloadTime) AS Fact)
      AS max_hits_latencyTracking_pageDownloadTime,
    (SELECT MAX(value) FROM UNNEST(Windows.hits_latencyTracking_pageLoadTime) AS Fact)
      AS max_hits_latencyTracking_pageLoadTime,

    # Features for categorical facts
    (
      SELECT IFNULL(COUNT(DISTINCT Fact.value), 0)
      FROM UNNEST(Windows.device_deviceCategory) AS Fact
    ) AS num_device_deviceCategory,
    (
      SELECT IFNULL(COUNT(Fact.value), 0)
      FROM UNNEST(Windows.hits_eCommerceAction_action_type) AS Fact
      WHERE CAST(Fact.value AS STRING) = '2'
    ) AS num_hits_eCommerceAction_action_type_product_view,
    (
      SELECT IFNULL(COUNT(Fact.value), 0)
      FROM UNNEST(Windows.hits_eCommerceAction_action_type) AS Fact
      WHERE CAST(Fact.value AS STRING) = '3'
    ) AS num_hits_eCommerceAction_action_type_add_to_cart,

    # Two different ways of computing the mode.
    (
      SELECT Fact.value
      FROM UNNEST(Windows.device_browser) AS Fact
      GROUP BY Fact.value ORDER BY COUNT(*) DESC, Fact.value LIMIT 1
    ) AS mode_device_browser,
    (
      SELECT APPROX_TOP_COUNT(Fact.value, 1)[OFFSET(0)].value
      FROM UNNEST(Windows.device_operatingSystem) AS Fact
    ) AS mode_device_operatingSystem,
    (
      SELECT Fact.value FROM UNNEST(Windows.session_hour) AS Fact ORDER BY ts DESC LIMIT 1
    ) AS latest_session_hour,
    (
      SELECT Fact.value
      FROM UNNEST(Windows.hits_eCommerceAction_action_type) AS Fact ORDER BY ts DESC LIMIT 1
    ) AS latest_hits_eCommerceAction_action_type,
    (
      SELECT Fact.value
      FROM UNNEST(Windows.device_isMobile) AS Fact ORDER BY ts DESC LIMIT 1
    ) AS latest_device_isMobile,

    SAFE_DIVIDE(
      (
        SELECT COUNT(Fact.value)
        FROM UNNEST(Windows.channelGrouping) AS Fact
        WHERE Fact.value = 'Direct'
      ),
      (SELECT COUNT(*) FROM UNNEST(Windows.channelGrouping))
    ) AS proportion_channelGrouping_Direct,
    SAFE_DIVIDE(
      (
        SELECT COUNT(Fact.value)
        FROM UNNEST(Windows.channelGrouping) AS Fact
        WHERE Fact.value = 'Organic Search'
      ),
      (SELECT COUNT(*) FROM UNNEST(Windows.channelGrouping))
    ) AS proportion_channelGrouping_Organic_Search,
    SAFE_DIVIDE(
      (
        SELECT COUNT(Fact.value)
        FROM UNNEST(Windows.channelGrouping) AS Fact
        WHERE Fact.value = 'Paid Search'
      ),
      (SELECT COUNT(*) FROM UNNEST(Windows.channelGrouping))
    ) AS proportion_channelGrouping_Paid_Search,
    SAFE_DIVIDE(
      (
        SELECT COUNT(Fact.value)
        FROM UNNEST(Windows.channelGrouping) AS Fact
        WHERE Fact.value = 'Referral'
      ),
      (SELECT COUNT(*) FROM UNNEST(Windows.channelGrouping))
    ) AS proportion_channelGrouping_Referral,
    SAFE_DIVIDE(
      (
        SELECT COUNT(Fact.value) FROM UNNEST(Windows.channelGrouping) AS Fact
        WHERE Fact.value NOT IN ('Direct', 'Organic Search', 'Paid Search', 'Referral')
      ),
      (SELECT COUNT(*) FROM UNNEST(Windows.channelGrouping))
    ) AS proportion_channelGrouping_Others
  FROM
    `{{windows_table}}` AS Windows
);
