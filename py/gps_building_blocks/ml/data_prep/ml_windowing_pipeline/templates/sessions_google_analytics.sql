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
# Sample SQL Jinja template to extract Google Analytics (GA) BigQuery data into an internal format.
# Args:
#   analytics_table: input Google Analytics BigQuery tablename.
#   sessions_table: output sessions BigQuery tablename.
#
# This is only a sample file. Override it with your own query that extracts the session information
# you want to train over. Every row/session in the Google Analytics BigQuery table has up to
# approximately 350 facts. For efficiency, only extract the data you need.
#
# Note that the input data is not limited to GA data. You can rewrite this query to extract data
# from Firebase, or even your own CRM data. The only requirement is that the output of the query
# matches the following schema:
#
# Output schema:
#   user_id: STRING
#   session_ts: TIMESTAMP (timestamp of the start of the session)
#   session_id: STRING (unique id for the session)
#
# And then any number of facts, where a fact is a STRUCT containing a value and a timestamp. All
# facts with the same name must be grouped together in the same array.
#   factname: STRUCT REPEATED
#   factname.value: ANY TYPE
#   factname.ts: TIMESTAMP
#
# Use the query below as a guide for how to extract facts. Take special note of how
# customDimensions and hits are extracted as they demonstrate two opposed approaches to preserving
# the order of events.
#
# In the input GA data, customDimensions is a repeated array of pair records, as below:
#  customDimensions	RECORD	REPEATED
#  customDimensions.index	INTEGER
#  customDimensions.value	STRING
# Note that the ith index represents one concept across all rows, e.g. CRM id of the customer, or
# country of origin, etc. The ith index has no relation to the index before or after it. To
# represent this as a fact, we create a fact name customDimensions<i>, with value equal to the
# ith value in the GA customDimensions array.
#
# Similarly, in the input GA data, hits are a repeated array of records. For this script, we are
# not interested in differentiating between the say first hit and second hit (although that could
# make a good feature). Instead, we just want to collect all pagePaths for example that were hit in
# the session, with no regard to the order. In the script below, give pagePath values the same fact
# name hits.page.pagePath so they are collected together in the same array. This contracts to the
# customDimensions approach where we might have facts like hits1.page.pagePath, hits2.page.pagePath
# and so on.

# Sample function to clean the URL data. Returns the given url string with any trailing / removed.
CREATE TEMP FUNCTION NormalizeUrl(url STRING)
RETURNS STRING
LANGUAGE js AS """
  if (url && url.length > 1 && url.endsWith('/')) {
    return url.slice(0, - 1);
  }
  return url;
""";

CREATE OR REPLACE TABLE `{{sessions_table}}`
AS (
  SELECT
    IFNULL(NULLIF(clientId, ''), fullVisitorId) AS user_id,
    TIMESTAMP_SECONDS(visitStartTime) AS session_ts,
    CONCAT(fullVisitorId, '/', visitId) AS session_id,

    # Extract facts from the GA Session data.
    # Google style note: Maintaining core fact name in lowerCamelCase to match the GA schema.
    -- totals
    [STRUCT(totals.visits AS value, TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS totals_visits,
    [STRUCT(totals.hits AS value, TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS totals_hits,
    [STRUCT(totals.pageviews AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS totals_pageviews,
    [STRUCT(totals.timeOnSite AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS totals_timeOnSite,
    [STRUCT(totals.bounces AS value, TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS totals_bounces,
    [STRUCT(totals.transactions AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS totals_transactions,
    [STRUCT(totals.newVisits AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS totals_newVisits,
    [STRUCT(totals.screenviews AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS totals_screenviews,
    [STRUCT(totals.uniqueScreenviews AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS totals_uniqueScreenviews,
    [STRUCT(totals.timeOnScreen AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS totals_timeOnScreen,
    [STRUCT(totals.totalTransactionRevenue AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS totals_totalTransactionRevenue,
    [STRUCT(totals.sessionQualityDim AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS totals_sessionQualityDim,
    -- trafficSource
    [STRUCT(NormalizeUrl(trafficSource.referralPath) AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS trafficSource_referralPath,
    [STRUCT(trafficSource.campaign AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS trafficSource_campaign,
    [STRUCT(trafficSource.source AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS trafficSource_source,
    [STRUCT(trafficSource.medium AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS trafficSource_medium,
    [STRUCT(trafficSource.keyword AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS trafficSource_keyword,
    -- misc
    [STRUCT(FORMAT_TIMESTAMP('%a', TIMESTAMP_SECONDS(visitStartTime)) AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS session_day_of_week,
    [STRUCT(CAST(EXTRACT(HOUR FROM TIMESTAMP_SECONDS(visitStartTime)) AS STRING) AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS session_hour,
    [STRUCT(channelGrouping AS value, TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS channelGrouping,
    [STRUCT(socialEngagementType AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS socialEngagementType,
    -- device
    [STRUCT(device.browser AS value, TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS device_browser,
    [STRUCT(device.browserVersion AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS device_browserVersion,
    [STRUCT(device.browserSize AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS device_browserSize,
    [STRUCT(device.operatingSystem AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS device_operatingSystem,
    [STRUCT(device.operatingSystemVersion AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS device_operatingSystemVersion,
    [STRUCT(device.isMobile AS value, TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS device_isMobile,
    [STRUCT(device.mobileDeviceBranding AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS device_mobileDeviceBranding,
    [STRUCT(device.mobileDeviceModel AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS device_mobileDeviceModel,
    [STRUCT(device.mobileDeviceInfo AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS device_mobileDeviceInfo,
    [STRUCT(device.mobileDeviceMarketingName AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS device_mobileDeviceMarketingName,
    [STRUCT(device.flashVersion AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS device_flashVersion,
    [STRUCT(device.javaEnabled AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS device_javaEnabled,
    [STRUCT(device.language AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS device_language,
    [STRUCT(device.screenColors AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS device_screenColors,
    [STRUCT(device.screenResolution AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS device_screenResolution,
    [STRUCT(device.deviceCategory AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS device_deviceCategory,
    -- geoNetwork
    [STRUCT(geoNetwork.continent AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS geoNetwork_continent,
    [STRUCT(geoNetwork.subContinent AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS geoNetwork_subContinent,
    [STRUCT(geoNetwork.country AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS geoNetwork_country,
    [STRUCT(geoNetwork.region AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS geoNetwork_region,
    [STRUCT(geoNetwork.metro AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS geoNetwork_metro,
    [STRUCT(geoNetwork.city AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS geoNetwork_city,
    [STRUCT(geoNetwork.cityId AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS geoNetwork_cityId,
    [STRUCT(geoNetwork.latitude AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS geoNetwork_latitude,
    [STRUCT(geoNetwork.longitude AS value,
            TIMESTAMP_SECONDS(visitStartTime) AS ts)] AS geoNetwork_longitude,
    -- customDimensions: Example of how to extract the 4th customDimension.
    --  ARRAY(
    --    SELECT
    --      STRUCT(customDimensions.value AS value, TIMESTAMP_SECONDS(visitStartTime) AS ts)
    --      FROM UNNEST(customDimensions) AS customDimensions
    --      WHERE
    --        customDimensions.index = 4
    --        AND customDimensions.value IS NOT NULL
    --  ) AS customDimensions4,
    -- hits.page
    ARRAY(
      SELECT
        STRUCT(NormalizeUrl(hits.page.pagePath) AS value,
               TIMESTAMP_MILLIS(visitStartTime * 1000 + hits.time) AS ts)
      FROM UNNEST(hits) AS hits
    ) AS hits_page_pathPath,
    ARRAY(
      SELECT
        STRUCT(hits.page.hostname AS value,
               TIMESTAMP_MILLIS(visitStartTime * 1000 + hits.time) AS ts)
      FROM UNNEST(hits) AS hits
    ) AS hits_page_hostname,
    ARRAY(
      SELECT
        STRUCT(hits.page.pageTitle AS value,
               TIMESTAMP_MILLIS(visitStartTime * 1000 + hits.time) AS ts)
      FROM UNNEST(hits) AS hits
    ) AS hits_page_pageTitle,
    ARRAY(
      SELECT
        STRUCT(hits.page.searchKeyword AS value,
               TIMESTAMP_MILLIS(visitStartTime * 1000 + hits.time) AS ts)
      FROM UNNEST(hits) AS hits
    ) AS hits_page_searchKeyword,
    ARRAY(
      SELECT
        STRUCT(hits.page.searchCategory AS value,
               TIMESTAMP_MILLIS(visitStartTime * 1000 + hits.time) AS ts)
      FROM UNNEST(hits) AS hits
    ) AS hits_page_searchCategory,
    ARRAY(
      SELECT
        STRUCT(NormalizeUrl(hits.page.pagePathLevel1) AS value,
               TIMESTAMP_MILLIS(visitStartTime * 1000 + hits.time) AS ts)
      FROM UNNEST(hits) AS hits
    ) AS hits_page_pathlevel1,
    ARRAY(
      SELECT
        STRUCT(NormalizeUrl(hits.page.pagePathLevel2) AS value,
               TIMESTAMP_MILLIS(visitStartTime * 1000 + hits.time) AS ts)
      FROM UNNEST(hits) AS hits
    ) AS hits_page_pathlevel2,
    ARRAY(
      SELECT
        STRUCT(NormalizeUrl(hits.page.pagePathLevel3) AS value,
               TIMESTAMP_MILLIS(visitStartTime * 1000 + hits.time) AS ts)
      FROM UNNEST(hits) AS hits
    ) AS hits_page_pathlevel3,
    ARRAY(
      SELECT
        STRUCT(NormalizeUrl(hits.page.pagePathLevel4) AS value,
               TIMESTAMP_MILLIS(visitStartTime * 1000 + hits.time) AS ts)
      FROM UNNEST(hits) AS hits
    ) AS hits_page_pathlevel4,
    -- hits.eventInfo
    ARRAY(
      SELECT
        STRUCT(hits.eventInfo.eventCategory AS value,
               TIMESTAMP_MILLIS(visitStartTime * 1000 + hits.time) AS ts)
      FROM UNNEST(hits) AS hits
    ) AS hits_eventInfo_eventCategory,
    ARRAY(
      SELECT
        STRUCT(hits.eventInfo.eventAction AS value,
               TIMESTAMP_MILLIS(visitStartTime * 1000 + hits.time) AS ts)
      FROM UNNEST(hits) AS hits
    ) AS hits_eventInfo_eventAction,
    -- hits.misc
    ARRAY(
      SELECT
        STRUCT(hits.refund.refundAmount AS value,
               TIMESTAMP_MILLIS(visitStartTime * 1000 + hits.time) AS ts)
      FROM UNNEST(hits) AS hits
    ) AS hits_refund_refundAmount,
    ARRAY(
      SELECT
        STRUCT(hits.eCommerceAction.action_type AS value,
               TIMESTAMP_MILLIS(visitStartTime * 1000 + hits.time) AS ts)
      FROM UNNEST(hits) AS hits
    ) AS hits_eCommerceAction_action_type,
    -- hits.latencyTracking
    ARRAY(
      SELECT
        STRUCT(hits.latencyTracking.pageDownloadTime AS value,
               TIMESTAMP_MILLIS(visitStartTime * 1000 + hits.time) AS ts)
      FROM UNNEST(hits) AS hits
    ) AS hits_latencyTracking_pageDownloadTime,
    ARRAY(
      SELECT
        STRUCT(hits.latencyTracking.pageLoadTime AS value,
               TIMESTAMP_MILLIS(visitStartTime * 1000 + hits.time) AS ts)
      FROM UNNEST(hits) AS hits
    ) AS hits_latencyTracking_pageLoadTime,
    ARRAY(
      SELECT
        STRUCT(hits.latencyTracking.redirectionTime AS value,
               TIMESTAMP_MILLIS(visitStartTime * 1000 + hits.time) AS ts)
      FROM UNNEST(hits) AS hits
    ) AS hits_latencyTracking_redirectionTime,
    ARRAY(
      SELECT
        STRUCT(hits.latencyTracking.speedMetricsSample AS value,
               TIMESTAMP_MILLIS(visitStartTime * 1000 + hits.time) AS ts)
      FROM UNNEST(hits) AS hits
    ) AS hits_latencyTracking_speedMetricsSample,
    ARRAY(
      SELECT
        STRUCT(hits.latencyTracking.domainLookupTime AS value,
               TIMESTAMP_MILLIS(visitStartTime * 1000 + hits.time) AS ts)
      FROM UNNEST(hits) AS hits
    ) AS hits_latencyTracking_domainLookupTime,
    ARRAY(
      SELECT
        STRUCT(hits.latencyTracking.serverConnectionTime AS value,
               TIMESTAMP_MILLIS(visitStartTime * 1000 + hits.time) AS ts)
      FROM UNNEST(hits) AS hits
    ) AS hits_latencyTracking_serverConnectionTime,
    ARRAY(
      SELECT
        STRUCT(hits.latencyTracking.serverResponseTime AS value,
               TIMESTAMP_MILLIS(visitStartTime * 1000 + hits.time) AS ts)
      FROM UNNEST(hits) AS hits
    ) AS hits_latencyTracking_serverResponseTime,
    ARRAY(
      SELECT
        STRUCT(hits.latencyTracking.userTimingSample AS value,
               TIMESTAMP_MILLIS(visitStartTime * 1000 + hits.time) AS ts)
      FROM UNNEST(hits) AS hits
    ) AS hits_latencyTracking_userTimingSample,
    ARRAY(
      SELECT
        STRUCT(hits.latencyTracking.userTimingCategory AS value,
               TIMESTAMP_MILLIS(visitStartTime * 1000 + hits.time) AS ts)
      FROM UNNEST(hits) AS hits
    ) AS hits_latencyTracking_userTimingCategory,
    ARRAY(
      SELECT
        STRUCT(hits.latencyTracking.userTimingValue AS value,
               TIMESTAMP_MILLIS(visitStartTime * 1000 + hits.time) AS ts)
      FROM UNNEST(hits) AS hits
    ) AS hits_latencyTracking_userTimingValue,
    ARRAY(
      SELECT
        STRUCT(product.v2ProductCategory AS value,
               TIMESTAMP_MILLIS(visitStartTime * 1000 + hits.time) AS ts)
      FROM UNNEST(hits) AS hits, UNNEST(hits.product) AS product
    ) AS hits_product_v2ProductCategory,
    ARRAY(
      SELECT
        STRUCT(product.productBrand AS value,
               TIMESTAMP_MILLIS(visitStartTime * 1000 + hits.time) AS ts)
      FROM UNNEST(hits) AS hits, UNNEST(hits.product) AS product
    ) AS hits_product_productBrand,
  FROM
    `{{analytics_table}}`
  WHERE
    # Exclude user_ids that are NULL or empty. Otherwise, the NULL/empty user_id will aggregate
    # sessions from many users with an unknown id.
    IFNULL(NULLIF(clientId, ''), fullVisitorId) IS NOT NULL
    AND LOWER(IFNULL(NULLIF(clientId, ''), fullVisitorId)) NOT IN ('', 'null')
);
