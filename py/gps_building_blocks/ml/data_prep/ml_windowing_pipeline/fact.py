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

"""Represents the BigQuery SchemaField for a fact from templates/sessions.sql.

Functions for extracting the name and type of each fact from the output of
templates/sessions.sql, and for classifying facts as numeric or categorical.
"""

from typing import List, TypeVar
from google.cloud import bigquery

_CATEGORICAL_TYPES = ('STRING', 'BOOL', 'BOOLEAN')
_NUMERIC_TYPES = (
    'INT64', 'INTEGER', 'FLOAT64', 'FLOAT', 'NUMERIC', 'BIGNUMERIC')

F = TypeVar('F', bound='Fact')


class Fact:
  """Internal wrapper around the BigQuery SchemaField for a fact."""

  def __init__(self, name: str, bigquery_type: str):
    """Creates a Fact from its name and BigQuery type.

    Args:
      name: name of the fact.
      bigquery_type: BigQuery type of the fact.
    """
    self.name = name
    self.bigquery_type = bigquery_type

  @classmethod
  def extract_facts(cls, sessions_table: bigquery.table.Table) -> List[F]:
    """Constructs and returns the list of Facts in the sessions_table schema.

    Args:
      sessions_table: Internal BigQuery table output by templates/sessions.sql.
    Returns:
      List of Facts with the name and type for each fact in the sessions_table.
    """
    facts = []
    for field in sessions_table.schema:
      if field.field_type != 'RECORD' or field.mode != 'REPEATED':
        continue
      contains_value = False
      field_type = ''
      contains_ts = False
      for child_field in field.fields:
        if child_field.name == 'value':
          contains_value = True
          field_type = child_field.field_type
        if child_field.name == 'ts':
          contains_ts = True
      if not contains_value or not contains_ts:
        continue
      facts.append(Fact(field.name, field_type))
    return facts

  @classmethod
  def get_numeric_facts(cls, facts: List[F]) -> List[F]:
    """Returns the subset of numeric facts in the given facts list."""
    return [fact for fact in facts if fact.bigquery_type in _NUMERIC_TYPES]

  @classmethod
  def get_categorical_facts(cls, facts: List[F]) -> List[F]:
    """Returns the subset of categorical facts in the given facts list."""
    return [fact for fact in facts if fact.bigquery_type in _CATEGORICAL_TYPES]
