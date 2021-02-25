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
# SQL Jinja template to create a mapping from top categorical fact values to BigQuery column names.
# Note that fact values may not be legal BigQuery column names. This script generates a new legal
# column name with the format <fact.name>_<encoded_value>, where encoded_value is the output of the
# ColumnName function in fact_value_to_column_name.sql
# Args:
#   categorical_facts: list of categorical Facts (fact.py) with a type that can be cast to STRING
#   categorical_facts_table: input BigQuery table name all categorical facts.
#   top_n_values_per_fact: output the top_n_values_per_fact by descending count
#   categorical_fact_value_to_column_name_table: output BigQuery tablename to write the
# mappings

{% include 'fact_value_to_column_name.sql' %}
CREATE OR REPLACE TABLE `{{categorical_fact_value_to_column_name_table}}` (
  fact_name STRING,
  string_value STRING,
  value_count INT64,
  rank_by_count INT64,
  column_name_suffix STRING
);
{% if categorical_facts %}
INSERT INTO `{{categorical_fact_value_to_column_name_table}}`
# Note that the nested SELECT structure below is required because the HAVING clause does not work
# with analytic functions like ROW_NUMBER.
{% for fact in categorical_facts %}
  SELECT
    '{{fact.name}}' AS fact_name,
    CAST(TopCounts.value AS STRING) AS string_value,
    TopCounts.count AS count,
    ROW_NUMBER() OVER (ORDER BY count DESC) AS rank_by_count,
    ColumnName('{{fact.name}}', CAST(TopCounts.value AS STRING)) AS column_name_suffix
  FROM UNNEST ((
    # Using APPROX_TOP_COUNT instead of SELECT ORDER BY LIMIT for performance on larger datasets.
    SELECT APPROX_TOP_COUNT(Values.value, {{top_n_values_per_fact}})
    FROM (
      SELECT value
      FROM `{{categorical_facts_table}}`
      WHERE name='{{fact.name}}'
    ) AS Values
  )) AS TopCounts
  {% if not loop.last %}
  UNION ALL
  {% endif %}
{% endfor %}
;
{% endif %}
