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
# SQL Jinja template to extract mappings from fact values to encoded bigquery column name suffixes.
# Args:
#   categorical_fact_value_to_column_name_table: input BigQuery tablename from the output of
#     rank_categorical_fact_values_by_count.sql
SELECT fact_name, string_value, column_name_suffix
FROM `{{categorical_fact_value_to_column_name_table}}`
