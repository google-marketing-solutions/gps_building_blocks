# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Common utility functions for ML modules."""
import logging
import os
import sys
from typing import Dict, Optional, Union

import jinja2
import numpy as np
import sqlparse

logging.basicConfig(
    format='%(levelname)s: %(message)s', level=logging.INFO, stream=sys.stdout)


def assert_label_values_are_valid(labels: np.ndarray) -> None:
  """Asserts whether labels contains both and only 1.0 and 0.0 values.

  Args:
    labels: An array of true binary labels represented by 1.0 and 0.0.
  """
  assert set(labels) == {
      0.0, 1.0
  }, ('labels should contain both and only 1.0 and 0.0 values.')


def assert_prediction_values_are_valid(predictions: np.ndarray) -> None:
  """Asserts predictions contains values between 0.0 and 1.0.

  Args:
    predictions: An array of predicted probabilities between 0.0 and 1.0.
  """
  assert ((0.0 <= min(predictions)) and (max(predictions) <= 1.0)), (
      'probability_predictions should only contain values between 0.0 and 1.0')


def assert_label_and_prediction_length_match(labels: np.ndarray,
                                             predictions: np.ndarray) -> None:
  """Asserts labels and predictions have the same length.

  Args:
    labels: An array of true binary labels represented by 1.0 and 0.0.
    predictions: An array of predicted probabilities between 0.0 and 1.0.
  """
  assert len(labels) == len(predictions), (
      'labels and predictions should have the same length')


def read_file(file_path: str) -> str:
  """Reads and returns contents of the file.

  Args:
    file_path: File path.

  Returns:
    content: File content.

  Raises:
      FileNotFoundError: If the provided file is not found.
  """
  try:
    with open(file_path, 'r') as stream:
      content = stream.read()
  except FileNotFoundError:
    raise FileNotFoundError(f'The file "{file_path}" could not be found.')
  else:
    return content


def configure_sql(sql_path: str, query_params: Dict[str, Union[str, int,
                                                               float]]) -> str:
  """Configures parameters of SQL script with variables supplied from Airflow.

  Args:
    sql_path: Path to SQL script.
    query_params: Configuration containing query parameter values.

  Returns:
    sql_script: String representation of SQL script with parameters assigned.
  """
  sql_script = read_file(sql_path)

  params = {}
  for param_key, param_value in query_params.items():
    # If given value is list of strings (ex. 'a,b,c'), create tuple of
    # strings (ex. ('a', 'b', 'c')) to pass to SQL IN operator.
    if isinstance(param_value, str) and ',' in param_value:
      params[param_key] = tuple(param_value.split(','))
    else:
      params[param_key] = param_value

  return sql_script.format(**params)


def render_jinja_sql(template_dir: str,
                     template_name: str,
                     verbose: Optional[bool] = False,
                     **kwargs) -> str:
  """Renders the template sql file into a formatted sql statement.

  NOTE: The function doesn't do SQL injection filtering and not meant to be used
  for untrusted input.

  Args:
    template_dir: Directory containing SQL jinja2 template files.
    template_name: Base name of the SQL jinja2 file.
    verbose: Rendered SQL script is printed if set True.
     **kwargs: Parameters to insert into the template.

  Returns:
     Parametrized SQL query.
  """
  template_file_path = os.path.join(
      os.path.abspath(f'{template_dir}'), f'{template_name}.sql.jinja2')
  sql_template_str = read_file(template_file_path)
  template = jinja2.Template(sql_template_str).render(**kwargs)
  if verbose:
    logging.info(sqlparse.format(template, reindent=True, keyword_case='upper'))
  return template
