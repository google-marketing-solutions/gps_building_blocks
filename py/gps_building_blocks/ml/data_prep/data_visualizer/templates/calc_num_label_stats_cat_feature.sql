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

-- Calculate statistics for numerical label values by the different category values
-- of a categorical feature in the Features table in BigQuery.
-- Features table is created by the FeaturesPipeline of the MLWindowingPipeline tool. For more info:
-- https://github.com/google/gps_building_blocks/tree/master/py/gps_building_blocks/ml/data_prep/ml_windowing_pipeline
--
-- Query expects following parameters:
--  bq_features_table: Full path to the Features Table in BigQuery. Ex: project.dataset.table.
--  label_column: Name of the label column.
--  sql_code_segment: An SQL code segment containing a comma separated list of column names
--  formatted as follows: STRUCT('column' AS feature, column AS value).
WITH
  FeatureStructTable AS (
    SELECT
      user_id,
      {label_column},
      [
        {sql_code_segment}
      ] AS feature_data
    FROM
      `{bq_features_table}`
  ),
  FeatureLongTable AS (
    SELECT
      user_id,
      {label_column},
      feature_data
    FROM
      FeatureStructTable
    CROSS JOIN
      UNNEST(FeatureStructTable.feature_data) AS feature_data
  )
SELECT
  feature_data.feature AS feature,
  feature_data.value AS value,
  --  Following columns are used to plot the box plots of feature distribution using
  --  https://matplotlib.org/stable/api/_as_gen/matplotlib.axes.Axes.bxp.html
  -- The mean value
  AVG(SAFE_CAST({label_column} AS FLOAT64)) AS mean,
  -- The stdev value
  STDDEV_SAMP(SAFE_CAST({label_column} AS FLOAT64)) AS stddev,
  -- The median value
  APPROX_QUANTILES(SAFE_CAST({label_column} AS FLOAT64), 100)[OFFSET(50)] AS med,
  -- The first quartile (25th percentile) value
  APPROX_QUANTILES(SAFE_CAST({label_column} AS FLOAT64), 100)[OFFSET(25)] AS q1,
  -- The third quartile (75th percentile) value
  APPROX_QUANTILES(SAFE_CAST({label_column} AS FLOAT64), 100)[OFFSET(75)] AS q3,
  -- Lower bound of the lower whisker (1st percentile) value
  APPROX_QUANTILES(SAFE_CAST({label_column} AS FLOAT64), 100)[OFFSET(1)] AS whislo,
  -- Upper bound of the upper whisker (99th percentile) value
  APPROX_QUANTILES(SAFE_CAST({label_column} AS FLOAT64), 100)[OFFSET(99)] AS whishi
 FROM FeatureLongTable
 GROUP BY feature, value;
