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
# Sample SQL Jinja template to extract Firebase Analytics BigQuery data into an internal format
# that is required as the input to the MLDWP.
# Args:
#   analytics_table: input Firebase Analytics BigQuery tablename.
#   sessions_table: output sessions BigQuery tablename.
#
# This is only a sample file. All Firebase events of a user in a day are grouped into 1 session.
# Override it with your own query to extracts more information that you want to train over.
#
# Note that the input data is not limited to Firebase data. The only requirement is that the output
# of the query matches the following schema:
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

# Sample function to extract event parameter string value. Returns an array of string value.
CREATE TEMP FUNCTION ExtractEventParamValue(
  paramKey STRING, eventParams ANY TYPE, eventTimestamp INT64)
AS (
  ARRAY(
    SELECT
      STRUCT(
        params.value.string_value AS value,
        TIMESTAMP_MICROS(eventTimestamp) AS ts)
        AS event_name,
    FROM UNNEST(eventParams) AS params
    WHERE params.key = paramKey
  )
);

CREATE TEMP FUNCTION ExtractEventParamDoubleValue(
  paramKey STRING, eventParams ANY TYPE, eventTimestamp INT64)
AS (
  ARRAY(
    SELECT
      STRUCT(
        params.value.double_value AS value,
        TIMESTAMP_MICROS(eventTimestamp) AS ts)
        AS event_name,
    FROM UNNEST(eventParams) AS params
    WHERE params.key = paramKey
  )
);

CREATE OR REPLACE TABLE `{{sessions_table}}`
AS (
  SELECT
    COALESCE(user_id, user_pseudo_id) AS user_id,
    TIMESTAMP_MICROS(MIN(event_timestamp)) AS session_ts,
    CONCAT(user_pseudo_id, '/', event_date) AS session_id,
    -- Event
    ARRAY_AGG(
      STRUCT(event_name AS value, TIMESTAMP_MICROS(event_timestamp) AS ts))
      AS event_name,
    -- Event Param
    ARRAY_CONCAT_AGG(
      ExtractEventParamValue('source', event_params, event_timestamp))
      AS event_param_source,
    ARRAY_CONCAT_AGG(
      ExtractEventParamValue(
        'previous_os_version', event_params, event_timestamp))
      AS event_param_previous_os_version,
    ARRAY_CONCAT_AGG(
      ExtractEventParamValue('product_id', event_params, event_timestamp))
      AS event_param_product_id,
    ARRAY_CONCAT_AGG(
      ExtractEventParamDoubleValue('score', event_params, event_timestamp))
      AS event_param_score,
    -- Geo
    ARRAY_AGG(
      CASE
        WHEN geo.country IS NOT NULL
          THEN
            STRUCT(
              geo.country AS value, TIMESTAMP_MICROS(event_timestamp) AS ts)
        ELSE NULL
        END IGNORE NULLS)
      AS geo_country,
    ARRAY_AGG(
      CASE
        WHEN geo.city IS NOT NULL
          THEN
            STRUCT(geo.city AS value, TIMESTAMP_MICROS(event_timestamp) AS ts)
        ELSE NULL
        END IGNORE NULLS)
      AS geo_city,
    ARRAY_AGG(
      CASE
        WHEN geo.metro IS NOT NULL
          THEN
            STRUCT(geo.metro AS value, TIMESTAMP_MICROS(event_timestamp) AS ts)
        ELSE NULL
        END IGNORE NULLS)
      AS geo_metro,
    -- trafficSource
    ARRAY_AGG(
      CASE
        WHEN traffic_source.name IS NOT NULL
          THEN
            STRUCT(
              traffic_source.name AS value,
              TIMESTAMP_MICROS(event_timestamp) AS ts)
        ELSE NULL
        END IGNORE NULLS)
      AS trafficSource_name,
    ARRAY_AGG(
      CASE
        WHEN traffic_source.medium IS NOT NULL
          THEN
            STRUCT(
              traffic_source.medium AS value,
              TIMESTAMP_MICROS(event_timestamp) AS ts)
        ELSE NULL
        END IGNORE NULLS)
      AS trafficSource_medium,
    ARRAY_AGG(
      CASE
        WHEN traffic_source.source IS NOT NULL
          THEN
            STRUCT(
              traffic_source.source AS value,
              TIMESTAMP_MICROS(event_timestamp) AS ts)
        ELSE NULL
        END IGNORE NULLS)
      AS trafficSource_source,
  FROM `{{analytics_table}}`
  WHERE (user_pseudo_id IS NOT NULL OR user_id IS NOT NULL)
  GROUP BY user_id, session_id
);
