# python3
# coding=utf-8
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

"""Contains functions to support feature generation."""

import csv
import io
from typing import List
from gps_building_blocks.ml.data_prep.ml_windowing_pipeline import feature_option


def parse_feature_option(param_str: str) -> List[feature_option.FeatureOption]:
  """Parses a feature generation string parameter into a List of FeatureOption.

  `param_str` must be a semicolon-separated list of fact details. A fact detail
  should be in the format of `fact_name:[list_of_values]:[default_value]`.

    fact_name: Fact Name
      - It must contain only letters (a-z, A-Z), numbers (0-9), or underscores
      (_).

    list_of_values : Optional. Comma-separated list of values to consider.
      - If a value contains semi-colon, comma, brackets and colon, escape it
      with `///`.

    default_value : Default value. Optional.
      - Should not be in list_of_values.
      - If it contains semi-colon, comma, brackets and colon, escape it with
      `///`.

  Examples:
    device.browser:[Chrome,Firefox,Safari]:[Others]
    device_isMobile:[false,true]:[Others];session_hour
    totals_visits;totals_hits;totals_pageviews;totals_timeOnSite

  Args:
    param_str: feature generation parameter

  Returns:
    List of FeatureOptions.
  """

  features = []
  if not param_str:
    return features
  for param in split(param_str, ';'):
    components = split(param, ':')
    assert len(components) in (
        1, 3), 'Feature option parameter cannot be parsed: ' + param
    fact_name = components[0]
    if len(components) == 1:
      features.append(feature_option.FeatureOption(fact_name, {}, None))
      continue
    # Parse values
    values = split(components[1].strip('[]'), ',')
    # Parse remainder_value and construct remainder_column_name
    remainder_value = components[2].lstrip('[').rstrip(']')
    assert remainder_value not in values, (
        'Remainder value {} appears in value list {}').format(
            remainder_value, values)
    remainder_column_name = None
    if remainder_value:
      remainder_column_name = fact_name + '_' + remainder_value
    features.append(
        feature_option.FeatureOption(fact_name, values, remainder_column_name))
  return features


def split(param_str: str, delimiter: str) -> List[str]:
  values = []
  value_list_str = io.StringIO(param_str)
  for line in csv.reader(value_list_str, delimiter=delimiter, escapechar='/'):
    values = line
  return values


def merge_feature_option_list(
    list1: List[feature_option.FeatureOption],
    list2: List[feature_option.FeatureOption]
) -> List[feature_option.FeatureOption]:
  """Merges two lists of FeatureOption into one list without duplication.

  Args:
    list1: List of FeatureOptions
    list2: List of FeatureOptions

  Returns:
    List of FeatureOptions.
  """

  fact_dict = {}
  for feature_opt2 in list2:
    fact_dict[feature_opt2.fact_name] = feature_opt2

  for feature_opt1 in list1:
    if feature_opt1.fact_name not in fact_dict:
      fact_dict[feature_opt1.fact_name] = feature_opt1

  return list(fact_dict.values())
