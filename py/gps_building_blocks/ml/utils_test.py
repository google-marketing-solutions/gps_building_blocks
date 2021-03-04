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
"""Tests for gps_building_blocks.ml.utils."""

from absl.testing import absltest

import numpy as np
from gps_building_blocks.ml import utils


class UtilsTest(absltest.TestCase):

  def setUp(self):
    super(UtilsTest, self).setUp()
    self.test_sql = 'SELECT * FROM {project}.{dataset}.{table};'

  def test_assert_label_values_are_valid_raises_right_error(self):
    with self.assertRaises(AssertionError):
      utils.assert_label_values_are_valid(
          np.array([1.0, 1.0, 1.0, 0.0, 0.0, 4.0]))

  def test_assert_prediction_values_are_valid_raises_right_error(self):
    with self.assertRaises(AssertionError):
      utils.assert_prediction_values_are_valid(
          np.array([0.0, 0.5, 0.33, 0.1, 2.0]))

  def test_assert_label_and_prediction_length_match_right_error(self):
    with self.assertRaises(AssertionError):
      utils.assert_label_and_prediction_length_match(
          np.array([0.0, 0.0, 1.0, 1.0]), np.array([0.0, 0.5, 0.33]))

  def test_read_file_parses_file_content(self):
    test_content = 'test content'
    mock_open = absltest.mock.mock_open(read_data=test_content)

    with absltest.mock.patch('builtins.open', mock_open):
      actual = utils.read_file('/tmp/test_file.txt')
      self.assertEqual(actual, test_content)

  def test_read_file_raises_error_when_file_not_found(self):
    with self.assertRaises(FileNotFoundError):
      utils.read_file('non_existent_file')

  def test_congigure_sql_replaces_params(self):
    test_sql = 'SELECT * FROM {project}.{dataset}.{table};'
    query_params = {
        'project': 'test_project',
        'dataset': 'test_dataset',
        'table': 'test_table'
    }
    mock_open = absltest.mock.mock_open(read_data=test_sql)
    expected_sql = test_sql.format(**query_params)

    with absltest.mock.patch('builtins.open', mock_open):
      actual = utils.configure_sql(self.test_sql, query_params)
      self.assertEqual(expected_sql, actual)

  def test_congigure_sql_creates_tuple_given_list_of_strings(self):
    test_sql = 'SELECT * FROM test_table WHERE test_column IN {test_list};'
    query_params = {'test_list': 'value1,value2'}
    mock_open = absltest.mock.mock_open(read_data=test_sql)
    expected_sql = test_sql.format(test_list=('value1', 'value2'))

    with absltest.mock.patch('builtins.open', mock_open):
      actual = utils.configure_sql(self.test_sql, query_params)
      self.assertEqual(expected_sql, actual)


if __name__ == '__main__':
  absltest.main()
