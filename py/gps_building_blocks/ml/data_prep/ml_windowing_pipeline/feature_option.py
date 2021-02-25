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

"""Represents the user-provided configuration to generate features for MLWP."""


class FeatureOption:

  def __init__(self, fact_name, value_list, remainder_column_name):
    self.fact_name = fact_name
    self.value_list = value_list
    self.remainder_column_name = remainder_column_name

  def __str__(self):
    return ','.join(
        [self.fact_name,
         str(self.value_list), self.remainder_column_name])
