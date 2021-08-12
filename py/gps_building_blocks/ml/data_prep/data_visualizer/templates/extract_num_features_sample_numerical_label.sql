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

-- Extracts a random sample of rows for a given list of numerical features from the Features table
-- in BigQuery when the label is numerical.
-- Features table is created by the FeaturesPipeline of the MLWindowingPipeline tool. For more info:
-- https://github.com/google/gps_building_blocks/tree/master/py/gps_building_blocks/ml/data_prep/ml_windowing_pipeline
--
-- Query expects the following parameters:
-- bq_features_table: Full path to the Features Table in BigQuery. Ex: project.dataset.table.
-- label_column: Name of the label column.
-- num_instances: Number of rows randomly selected from the bq_features_table.
-- column_list_sql: An SQL code segment containing a comma separated list of column names.
--  Ex: colum_1, column_2, ...
SELECT
  {column_list_sql},
  {label_column}
FROM `{bq_features_table}`
WHERE RAND() < {num_instances} / (SELECT COUNT(*)
                                  FROM `{bq_features_table}`);
