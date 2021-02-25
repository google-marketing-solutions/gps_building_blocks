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
# SQL Jinja template to output categorical facts from sessions_table as input for data visualization
# Args:
#   sessions_table: input BigQuery sessions tablename from the output of sessions.sql
#   categorical_facts: list of categorical Facts (fact.py) with a type that can be cast to STRING
#   categorical_facts_table: output BigQuery tablename to write the facts
#
# Output schema:
#   session_id: STRING
#   user_id: STRING
#   ts: TIMESTAMP  (timestamp of the fact)
#   name: STRING   (name of the fact)
#   value: STRING  (value of the fact converted to a STRING)

CREATE OR REPLACE TABLE `{{categorical_facts_table}}`
AS (
  {% for fact in categorical_facts %}
  SELECT
    Sessions.session_id,
    Sessions.user_id,
    {{fact.name}}.ts AS ts,
    '{{fact.name}}' AS name,
    CAST({{fact.name}}.value AS STRING) AS value
  FROM `{{sessions_table}}` AS Sessions, UNNEST({{fact.name}}) AS {{fact.name}}
      {% if not loop.last %}
  UNION ALL
      {% endif %}
  {% endfor %}
);
