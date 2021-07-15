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
--
-- Query expects the following parameters:
--  bq_instance_table: Full path to the Instance Table in BigQuery. Ex: project.dataset.table.
--  label_column: Name of the label column of the Instance table.
--  positive_class_label: SQL code segment for the label value represeting the positive class
--    instances. If the value type is string provide it within quotes as 'class 1' and if the
--    value type is boolean, float or int provide it with out quotes as True, 1 or 1.0.
--  negative_class_label: SQL code segment for the label value represeting the negative class
--    instances. If the value type is string provide it within quotes as 'class 2' and if the
--    value type is boolean, float or int provide it with out quotes as False, 0 or 0.0.
WITH
  Data AS (
    SELECT
      CAST(snapshot_ts AS DATE) AS snapshot_date,
      {label_column} as label
    FROM `{bq_instance_table}`
    ),
  PosData AS (
    SELECT
      snapshot_date,
      COUNT(*) AS pos_count
    FROM Data
    WHERE label = {positive_class_label}
    GROUP BY snapshot_date
  ),
  NegData AS (
    SELECT
      snapshot_date,
      COUNT(*) AS neg_count
    FROM Data
    WHERE  label = {negative_class_label}
    GROUP BY snapshot_date
  ),
  PosNegData AS (
    SELECT
      NegData.snapshot_date,
      IFNULL(pos_count, 0) AS pos_count,
      IFNULL(neg_count, 0) AS neg_count,
      IFNULL(pos_count, 0) + IFNULL(neg_count, 0) AS tot_count,
      CASE
        WHEN pos_count IS NULL AND neg_count IS NULL THEN NULL
        ELSE IFNULL(pos_count, 0) / (IFNULL(pos_count, 0) + IFNULL(neg_count, 0)) * 100
      END AS positive_percentage
    FROM NegData
    FULL JOIN PosData ON PosData.snapshot_date = NegData.snapshot_date)

SELECT * FROM PosNegData
ORDER BY snapshot_date;
