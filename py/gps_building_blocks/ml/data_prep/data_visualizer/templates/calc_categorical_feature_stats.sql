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

-- Calculate statistics for categorical features in the Features table in BigQuery.
-- Features table is created by the
-- GenerateFeaturesPipeline of the MLDataWindowingPipeline tool. For more info:
-- https://github.com/GoogleCloudPlatform/cloud-for-marketing/tree/master/marketing-analytics/predicting/ml-data-windowing-pipeline
--
-- Query expects following parameters:
-- `bq_features_table`: Full path to the Features Table in BigQuery. Ex: project.dataset.table.
-- `struct_column_list_sql`: An SQL code segment containing a comma separated list of column names
--  formatted as follows: STRUCT('column' AS feature, column AS value).
WITH
  FeatureStructTable AS (
    SELECT
      userId AS user_id,
      effectiveDate AS snapshot_date,
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
  ),
  DateValueCountTable AS (
    SELECT
      snapshot_date AS snapshot_date,
      feature_data.feature,
      feature_data.value,
      label,
      COUNT(*) AS count
    FROM
      FeatureLongTable
    GROUP BY
      snapshot_date,
      feature,
      label,
      value
      ),
  TotalCountTable AS (
  SELECT
    snapshot_date AS snapshot_date,
    feature_data.feature,
    label,
    COUNT(*) AS total
  FROM
    FeatureLongTable
  GROUP BY
    snapshot_date,
    feature,
    label ),
  DateValueAndTotalCountTable AS (
  SELECT
    DateValueCountTable.snapshot_date,
    DateValueCountTable.feature,
    DateValueCountTable.value,
    DateValueCountTable.label,
    DateValueCountTable.count,
    TotalCountTable.total
  FROM
    DateValueCountTable
  INNER JOIN
    TotalCountTable
  ON
    DateValueCountTable.snapshot_date = TotalCountTable.snapshot_date
    AND DateValueCountTable.feature = TotalCountTable.feature
    AND DateValueCountTable.label = TotalCountTable.label )
SELECT
  *,
  SAFE_DIVIDE(count, total) * 100 AS percentage
FROM
  DateValueAndTotalCountTable;
