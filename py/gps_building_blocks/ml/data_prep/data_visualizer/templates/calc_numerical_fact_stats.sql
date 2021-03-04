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

-- Calculate statistics for numerical variables in the Facts table in BigQuery.
-- Facts table is created by the
-- DataExplorationPipeline of the MLDataWindowingPipeline tool. For more info:
-- https://github.com/GoogleCloudPlatform/cloud-for-marketing/tree/master/marketing-analytics/predicting/ml-data-windowing-pipeline
--
-- Query expects following parameters:
--  bq_facts_table: Full path to the Facts Table in BigQuery. Ex: project.dataset.table.
--  numerical_fact_list: A comma separated list of numerical fact names to calculate statistics for.

SELECT
  FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP_MILLIS(timeInMillis)) AS date,
  name AS fact_name,
  COUNT(*) AS total_record_count,
  AVG(SAFE_CAST(value AS INT64)) AS average,
  STDDEV_SAMP(SAFE_CAST(value AS INT64)) AS stddev
FROM `{bq_facts_table}`
WHERE name IN {numerical_fact_list}
GROUP BY date, fact_name
ORDER BY date, fact_name;
