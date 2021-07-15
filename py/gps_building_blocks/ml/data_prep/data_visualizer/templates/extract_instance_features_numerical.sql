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

-- Extracts days_since_first_activity, days_since_latest_activity from Instance table in BigQuery
-- for <num_instances> number of rows selected randomly.
-- Instance table is created by the DataExplorationPipeline of the MLWindowingPipeline tool.
-- For more info:
-- https://github.com/google/gps_building_blocks/tree/master/py/gps_building_blocks/ml/data_prep/ml_windowing_pipeline
-- This SQL is used for numerical labels (of Regression problems).
--
-- Query expects the following parameters:
--  bq_instance_table: Full path to the Instance Table in BigQuery. Ex: project.dataset.table.
--  num_instances: Number of rows to select randomly.

SELECT
  days_since_first_activity,
  days_since_latest_activity
FROM `{bq_instance_table}`
WHERE RAND() < {num_instances} / (SELECT COUNT(*) FROM `{bq_instance_table}`);
