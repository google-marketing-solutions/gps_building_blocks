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
# SQL Jinja template to automatically generate features from windows.
# Args:
#   windows_table: input BigQuery table name containing windows, e.g. from sliding_windows.sql
#   numeric_facts: Iterable[Fact] of numeric facts computed by facts.py
#   categorical_facts: Iterable[Fact] of categorical facts computed by facts.py
#   fact_name_to_value_and_column_suffix: Mapping[str, Iterable[Tuple[str, str]] map
#     from fact_name to list of (string_value, column_suffix) pairs. Used to generate features
#     per fact value (as a string), with column_suffix as the legal BigQuery column name suffix.
#     This map can be generated from rank_categorical_fact_values_by_count, which extracts the
#     top N values by count per categorical fact.
#   prediction_mode: True for no label output, and False otherwise.
#   features_table: output BigQuery table name to write the features.
#
# Implementation Note:
# The simplest and fastest implementation for feature extraction consists of one large query, with
# one subquery for each feature column. However, even for relatively small examples, this query will
# be too large for the BigQuery planner (even if the query is fast to run).
# Another option is to extract the features, one type at a time, and then to join the subtables into
# one large table. When there are lots of windows, the joins can be very slow.
# This implementation builds the features up one type at a time. It keeps all the window facts, so
# that the next feature can reference them when the feature columns are added. This means the window
# facts and previous features get written over and over again as the set of features is built up.
# The main advantage of this approach is that there are no joins and so it is much faster.

CREATE TEMP TABLE NumericFactFeatures AS (
  SELECT
    *,
    {% for fact in numeric_facts %}
    (SELECT IFNULL(SUM(value), 0) FROM UNNEST({{fact.name}})) AS sum_{{fact.name}},
    (SELECT IFNULL(AVG(value), 0) FROM UNNEST({{fact.name}})) AS avg_{{fact.name}},
    (
      SELECT
        SAFE_DIVIDE(IFNULL(SUM(value), 0), TIMESTAMP_DIFF(MAX(ts), MIN(ts), DAY))
      FROM UNNEST({{fact.name}})
    ) AS avg_by_day_{{fact.name}},
    (SELECT MAX(value) FROM UNNEST({{fact.name}})) AS max_{{fact.name}},
    (SELECT MIN(value) FROM UNNEST({{fact.name}})) AS min_{{fact.name}},
    (SELECT value FROM UNNEST({{fact.name}}) ORDER BY ts DESC LIMIT 1) AS latest_{{fact.name}},
    {% endfor %}
  FROM `{{windows_table}}`
);

CREATE TEMP TABLE CategoricalFactAndNumericFactFeatures AS (
  SELECT
    *,
    {% for fact in categorical_facts %}
    (SELECT value FROM UNNEST({{fact.name}}) ORDER BY ts DESC LIMIT 1) AS latest_{{fact.name}},
    (
      SELECT value FROM UNNEST({{fact.name}})
      GROUP BY value ORDER BY COUNT(*) DESC, value LIMIT 1
    ) AS mode_{{fact.name}},
    (SELECT COUNT(*) FROM UNNEST({{fact.name}})) AS count_{{fact.name}},
    (SELECT COUNT(DISTINCT value) FROM UNNEST({{fact.name}})) AS count_distinct_{{fact.name}},
    {% endfor %}
  FROM NumericFactFeatures
);

CREATE TEMP TABLE CategoricalValueAndCategoricalFactAndNumericFactFeatures AS (
  SELECT
    *,
    {% for fact_name, value_column_suffix_pairs in fact_name_to_value_and_column_suffix.items() %}
      {% for value, column_suffix in value_column_suffix_pairs %}
    (
      SELECT COUNT(*)
      FROM UNNEST({{fact_name}})
      WHERE
        CAST(value AS STRING) =
          {% if "'" not in value.__str__() %}
            '{{value}}'
          {% else %}
            JSON_EXTRACT_SCALAR('{{value | tojson}}', '$')
          {% endif %}
    ) AS count_{{column_suffix}},
      {% endfor %}
    (
      SELECT COUNT(value)
      FROM UNNEST({{fact_name}})
      WHERE
        CAST(value AS STRING) NOT IN
          (
            {% for value, _ in value_column_suffix_pairs %}
              {% if "'" not in value.__str__() %}
                '{{value}}'
              {% else %}
                JSON_EXTRACT_SCALAR('{{value | tojson}}', '$')
              {% endif %}
              {{ "," if not loop.last }}
            {%endfor%}
          )
    ) AS count_{{fact_name}}_others,
    {% endfor %}
  FROM CategoricalFactAndNumericFactFeatures
);

CREATE OR REPLACE TABLE `{{features_table}}` AS (
  SELECT
    Features.window_start_ts,
    Features.window_end_ts,
    Features.snapshot_ts,
    Features.user_id,
    {% if not prediction_mode %}
    Features.label,
    {% endif %}

    # Out-of-window activity features.
    (
      SELECT TIMESTAMP_DIFF(Features.snapshot_ts, MIN(Sessions.session_ts), DAY)
      FROM `{{sessions_table}}` AS Sessions
      WHERE
        Sessions.user_id = Features.user_id
        AND Sessions.session_ts < Features.snapshot_ts
    ) AS days_since_first_activity,
    (
      SELECT TIMESTAMP_DIFF(Features.snapshot_ts, MAX(Sessions.session_ts), DAY)
      FROM `{{sessions_table}}` AS Sessions
      WHERE
        Sessions.user_id = Features.user_id
        AND Sessions.session_ts < Features.snapshot_ts
    ) AS days_since_latest_activity,
    # Seasonality / Time based features
    CONCAT(EXTRACT(DAYOFWEEK FROM Features.snapshot_ts), 'D') AS snapshot_ts_day_of_week,
    CONCAT(EXTRACT(ISOWEEK FROM Features.snapshot_ts), 'W') AS snapshot_ts_week_of_year,
    CONCAT(EXTRACT(MONTH FROM Features.snapshot_ts), 'M') AS snapshot_ts_month_of_year,

    # Features for numerical facts
    {% for fact in numeric_facts %}
    Features.sum_{{fact.name}},
    Features.avg_{{fact.name}},
    Features.avg_by_day_{{fact.name}},
    Features.max_{{fact.name}},
    Features.min_{{fact.name}},
    Features.latest_{{fact.name}},
    {% endfor %}

    # Features for categorical facts
    {% for fact in categorical_facts %}
    Features.latest_{{fact.name}},
    Features.mode_{{fact.name}},
    Features.count_{{fact.name}},
    Features.count_distinct_{{fact.name}},
    {% endfor %}

    # Features for categorical fact values
    {% for fact_name, value_column_suffix_pairs in fact_name_to_value_and_column_suffix.items() %}
      {% for value, column_suffix in value_column_suffix_pairs %}
    Features.count_{{column_suffix}},
    SAFE_DIVIDE(Features.count_{{column_suffix}}, Features.count_{{fact_name}})
      AS proportion_{{column_suffix}},
      {% endfor %}
    Features.count_{{fact_name}}_others,
    SAFE_DIVIDE(Features.count_{{fact_name}}_others, Features.count_{{fact_name}})
      AS proportion_{{fact_name}}_others,
    {% endfor %}
  FROM CategoricalValueAndCategoricalFactAndNumericFactFeatures AS Features
);
