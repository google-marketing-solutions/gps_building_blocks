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
#   feature_options: List of feature option with value list

{% include 'fact_value_to_column_name.sql' %}
{% for feature_option in count_proportion_feature_options  %}
  {% if not loop.first and feature_option.value_list %} # Union at Feature Option level
   UNION ALL
  {% endif %}
  {% for value in feature_option.value_list  %}
  SELECT
    '{{feature_option.fact_name}}' AS fact_name,
    '{{value}}' AS fact_value,
    ColumnName('{{feature_option.fact_name}}', '{{value}}') AS column_name_suffix,
    {% if not loop.last %}  # Union at value level
  UNION ALL
    {% endif %}
  {% endfor %}
{% endfor %}
;
