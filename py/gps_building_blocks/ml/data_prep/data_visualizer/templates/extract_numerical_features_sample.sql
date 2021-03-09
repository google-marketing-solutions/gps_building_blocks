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
-- in BigQuery. Features table is created by the GenerateFeaturesPipeline of the
-- MLDataWindowingPipeline tool. For more info:
-- https://github.com/GoogleCloudPlatform/cloud-for-marketing/tree/master/marketing-analytics/predicting/ml-data-windowing-pipeline
--
-- Query expects the following parameters:
-- `bq_features_table`: Full path to the Features Table in BigQuery. Ex: project.dataset.table.
-- 'num_pos_instances': Number of rows randomly selected from the positive instances where
--    hasPositiveLabel column has value 'True'.
-- 'num_neg_instances': Number of rows randomly selected from the negative instances where
--    hasPositiveLabel column has value 'False'.
-- `column_list_sql`: An SQL code segment containing a comma separated list of column names.
--  Ex: colum_1, column_2, ...
WITH
  PositiveExamples AS (
    SELECT
      {column_list_sql},
      predictionLabel AS label
    FROM `{bq_features_table}`
    WHERE
      predictionLabel = TRUE
      AND RAND() < {num_pos_instances} / (SELECT COUNT(*)
                                          FROM `{bq_features_table}`
                                          WHERE predictionLabel = true)
  ),
  NegativeExamples AS (
    SELECT
      {column_list_sql},
      predictionLabel AS label
    FROM `{bq_features_table}`
    WHERE
      predictionLabel = FALSE
      AND RAND() < {num_neg_instances} / (SELECT COUNT(*)
                                        FROM `{bq_features_table}`
                                        WHERE predictionLabel = false)
  ),
  PositiveAndNegativeExamples AS (
    SELECT *
    FROM PositiveExamples
    UNION ALL
    SELECT *
    FROM NegativeExamples
  )
SELECT *
FROM PositiveAndNegativeExamples;