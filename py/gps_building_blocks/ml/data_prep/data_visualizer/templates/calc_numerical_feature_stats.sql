-- Copyright 2021 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Calculate statistics for numerical features in the Features table in BigQuery.
-- Features table is created by the
-- GenerateFeaturesPipeline of the MLDataWindowingPipeline tool. For more info:
-- https://github.com/GoogleCloudPlatform/cloud-for-marketing/tree/master/marketing-analytics/predicting/ml-data-windowing-pipeline
--
-- Query expects following parameters:
--  bq_features_table: Full path to the Features Table in BigQuery. Ex: project.dataset.table.
--  struct_column_list_sql: An SQL code segment containing a comma separated list of column names
--  formatted as follows: STRUCT('column' AS feature, column AS value).
WITH
  FeatureStructTable AS (
    SELECT
      userId AS user_id,
      FORMAT_DATETIME("%Y-%m-%d", effectiveDate) AS snapshot_date,
      predictionLabel AS label,
      [
        {sql_code_segment}
      ] AS feature_data
    FROM
      `{bq_features_table}`
  ),
  FeatureLongTable AS (
    SELECT
      user_id,
      snapshot_date,
      label,
      feature_data
    FROM
      FeatureStructTable
    CROSS JOIN
      UNNEST(FeatureStructTable.feature_data) AS feature_data
  )
SELECT
  snapshot_date,
  feature_data.feature AS feature,
  label,
  COUNT(*) AS record_count,
  1 - (COUNT(feature_data.value)/COUNT(*)) AS prop_missing,
  1 - (COUNT(SAFE_CAST(feature_data.value AS INT64))/COUNT(*)) AS prop_non_num,
  AVG(SAFE_CAST(feature_data.value AS INT64)) AS average,
  STDDEV_SAMP(SAFE_CAST(feature_data.value AS INT64)) AS stddev
 FROM FeatureLongTable
 GROUP BY snapshot_date, feature, label;
