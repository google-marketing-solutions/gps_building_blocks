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

-- Calculate statistics from Instance table in BigQuery. Instance table is created by the
-- DataExplorationPipeline of the MLWindowingPipeline tool. For more info:
-- https://github.com/google/gps_building_blocks/tree/master/py/gps_building_blocks/ml/data_prep/ml_windowing_pipeline
-- This SQL is used for numerical labels (of Regression problems).
-- Query expects the following parameters:
--  bq_instance_table: Full path to the Instance Table in BigQuery. Ex: project.dataset.table.
--  label_column: Name of the label column of the Instance table.
WITH
  Data AS (
    SELECT
      CAST(snapshot_ts AS DATE) AS snapshot_date,
      {label_column} AS label
    FROM `{bq_instance_table}`
  )
SELECT
  snapshot_date,
  AVG(label) AS mean,
  MAX(label) AS max,
  MIN(label) AS min,
  COUNT(label) AS tot_count,
  -- following column names are following: https://matplotlib.org/stable/api/_as_gen/matplotlib.axes.Axes.bxp.html

  CAST(snapshot_date AS STRING) AS name,
  -- The median value
  APPROX_QUANTILES(label, 100)[OFFSET(50)] AS med,
  -- The first quartile (25th percentile)
  APPROX_QUANTILES(label, 100)[OFFSET(25)] AS q1,
  -- The third quartile (75th percentile)
  APPROX_QUANTILES(label, 100)[OFFSET(75)] AS q3,
  -- Lower bound of the lower whisker
  APPROX_QUANTILES(label, 100)[OFFSET(1)] AS whislo,
  -- Upper bound of the upper whisker
  APPROX_QUANTILES(label, 100)[OFFSET(99)] AS whishi
FROM Data
GROUP BY snapshot_date
ORDER BY snapshot_date;
