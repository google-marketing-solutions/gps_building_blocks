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
# SQL Jinja template to generate features from windows using the feature options.
# Args:
#   windows_table: input BigQuery table name containing windows, e.g. from sliding_windows.sql
#   avg_feature_options: list of FeatureOptions for Average operation
#   sum_feature_options: list of FeatureOptions for Sum operation
#   count_feature_options: list of FeatureOptions for Count operation
#   proportions_feature_options: list of FeatureOptions for Proportion operation
#   recent_feature_options: list of FeatureOptions for Recent operation
#   mode_feature_options: list of FeatureOptions for Mode operation
#   fact_name_to_value_and_column_suffix: Mapping[str, Iterable[Tuple[str, str]] map
#     from fact_name to list of (string_value, column_suffix) pairs.
#   sessions_table: input BigQuery table name from the output of sessions_sql.
#   prediction_mode: True for no label output, and False otherwise.
#   features_table: output BigQuery table name to write the features.

CREATE TEMP TABLE FeaturesWithFactValueCount
AS (
  SELECT
    *,
    {% for feature_option in sum_feature_options %}
    (
      SELECT IFNULL(SUM(value), 0) FROM UNNEST({{feature_option.fact_name}})
    ) AS sum_{{feature_option.fact_name}},
    {% endfor %}
    {% for feature_option in avg_feature_options %}
    (
      SELECT IFNULL(AVG(value), 0) FROM UNNEST({{feature_option.fact_name}})
    ) AS avg_{{feature_option.fact_name}},
    {% endfor %}
    {% for feature_option in max_feature_options %}
    (
      SELECT MAX(value) FROM UNNEST({{feature_option.fact_name}})
    ) AS max_{{feature_option.fact_name}},
    {% endfor %}
    {% for feature_option in min_feature_options %}
    (
      SELECT MIN(value) FROM UNNEST({{feature_option.fact_name}})
    ) AS min_{{feature_option.fact_name}},
    {% endfor %}
    {% for feature_option in count_proportion_feature_options %}
    (
      SELECT COUNT(*) FROM UNNEST({{feature_option.fact_name}})
    ) AS count_{{feature_option.fact_name}},
    {% endfor %}
    {% for feature_option in mode_feature_options %}
    (
      SELECT
      {% if not feature_option.value_list %}
        value
      {% else %}
        CASE
        {% for value in feature_option.value_list %}
          WHEN CAST(value AS STRING) = '{{value}}' THEN CAST(value AS STRING)
        {% endfor %}
        {% if feature_option.remainder_column_name %}
          ELSE '{{feature_option.remainder_column_name}}'
        {% endif %}
        END
      {% endif %}
        AS transformed_value
      FROM UNNEST({{feature_option.fact_name}})
      GROUP BY transformed_value
      ORDER BY COUNT(*) DESC, transformed_value
      LIMIT 1
    ) AS mode_{{feature_option.fact_name}},
    {% endfor %}
    {% for feature_option in latest_feature_options %}
    (
      SELECT
      {% if not feature_option.value_list %}
        value
      {% else %}
        CASE
        {% for value in feature_option.value_list %}
          WHEN CAST(value AS STRING) = '{{value}}' THEN CAST(value AS STRING)
        {% endfor %}
        {% if feature_option.remainder_column_name %}
          ELSE '{{feature_option.remainder_column_name}}'
        {% endif %}
          END
      {% endif %}
        AS transformed_value
      FROM UNNEST({{feature_option.fact_name}})
      ORDER BY ts DESC
      LIMIT 1
     ) AS latest_{{feature_option.fact_name}},
    {% endfor %}
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
        CAST(value AS STRING)
        NOT IN (
     {% for value, _ in value_column_suffix_pairs %}
       {% if "'" not in value.__str__() %}
         '{{value}}'
       {% else %}
         JSON_EXTRACT_SCALAR('{{value | tojson}}', '$')
       {% endif %}
       {{"," if not loop.last}}
     {% endfor %})
    ) AS count_{{fact_name}}_others,
    {% endfor %}
  FROM `{{windows_table}}`
);

CREATE OR REPLACE TABLE `{{features_table}}`
AS (
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
    # Sum features
    {% for feature_option in sum_feature_options %}
    Features.sum_{{feature_option.fact_name}},
    {% endfor %}
    # Avg features
    {% for feature_option in avg_feature_options %}
    Features.avg_{{feature_option.fact_name}},
    {% endfor %}
    # Mode features
    {% for feature_option in mode_feature_options %}
    Features.mode_{{feature_option.fact_name}},
    {% endfor %}
    # Latest features
    {% for feature_option in latest_feature_options %}
    Features.latest_{{feature_option.fact_name}},
    {% endfor %}
    # Proportion features
    {% for opt in proportions_feature_options %}
      {% for value, column_suffix in fact_name_to_value_and_column_suffix[opt.fact_name]
            if value in opt.value_list %}
    SAFE_DIVIDE(Features.count_{{column_suffix}}, Features.count_{{opt.fact_name}})
        AS proportion_{{column_suffix}},
      {% endfor %}
      {% if opt.remainder_column_name %}
    SAFE_DIVIDE(
        Features.count_{{opt.fact_name}}_others,
        Features.count_{{opt.fact_name}})
      AS proportion_{{opt.remainder_column_name}},
      {% endif %}
    {% endfor %}
    # Count features
    {% for opt in count_feature_options%}
      {% if not opt.value_list %}
    Features.count_{{opt.fact_name}},
      {% else %}
        {% for value, column_suffix in fact_name_to_value_and_column_suffix[opt.fact_name]
            if value in opt.value_list %}
    Features.count_{{column_suffix}},
        {% endfor %}
        {% if opt.remainder_column_name %}
    Features.count_{{opt.fact_name}}_others AS count_{{opt.remainder_column_name}},
        {% endif %}
      {% endif %}
    {% endfor %}
    # Max features
    {% for feature_option in max_feature_options %}
    Features.max_{{feature_option.fact_name}},
    {% endfor %}
    # Min features
    {% for feature_option in min_feature_options %}
    Features.min_{{feature_option.fact_name}},
    {% endfor %}
  FROM FeaturesWithFactValueCount AS Features
);
