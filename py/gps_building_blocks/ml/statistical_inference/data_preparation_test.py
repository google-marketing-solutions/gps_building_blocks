# Copyright 2020 Google LLC
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

"""Tests for gps_building_blocks.ml.statistical_inference.inference."""

from unittest import mock

from absl.testing import parameterized
import numpy as np
import pandas as pd
from sklearn import datasets

import unittest
from gps_building_blocks.ml.statistical_inference import data_preparation


class InferenceTest(parameterized.TestCase):
  _missing_data = pd.DataFrame(
      data=[[np.nan, 0.0000],
            [0.6000, 0.0000],
            [0.4000, 3.0000],
            [0.2000, np.nan]],
      columns=['first', 'second'])

  def test_missing_value_emits_warning_twice(self):
    with self.assertWarns(data_preparation.MissingValueWarning):
      data_preparation.InferenceData(self._missing_data)
    with self.assertWarns(data_preparation.MissingValueWarning):
      data_preparation.InferenceData(self._missing_data)

  def test_check_data_raises_exception_on_missing_data(self):
    inference_data = data_preparation.InferenceData(self._missing_data)

    with self.assertRaises(data_preparation.MissingValueError):
      inference_data.data_check(raise_on_error=True)

  def test_invalid_target_column_raise_exception(self):
    with self.assertRaises(KeyError):
      data_preparation.InferenceData(
          initial_data=self._missing_data,
          target_column='non_ci_sono')

  def test_impute_missing_values_replaced_with_mean(self):
    inference_data = data_preparation.InferenceData(self._missing_data)
    expected_result = pd.DataFrame(
        data=[[0.4000, 0.0000],
              [0.6000, 0.0000],
              [0.4000, 3.0000],
              [0.2000, 1.0000]],
        columns=['first', 'second'])

    result = inference_data.impute_missing_values(strategy='mean')

    pd.testing.assert_frame_equal(result, expected_result)

  def test_fixed_effect_raise_exception_on_categorical_covariate(self):
    data = pd.DataFrame(
        data=[['0', 0.0, '1', 3.0],
              ['1', 0.0, '2', 2.0],
              ['1', 1.0, '3', 2.0],
              ['1', 1.0, '4', 1.0]],
        columns=['control_1', 'control_2', 'variable_1', 'variable_2'],
        index=['group1', 'group2', 'group3', 'group3'])
    inference_data = data_preparation.InferenceData(data)

    with self.assertRaises(data_preparation.CategoricalCovariateError):
      inference_data.control_with_fixed_effect(
          strategy='quick',
          control_columns=['control_1', 'control_2'],
          min_frequency=1)

  def test_fixed_effect_demeaning_subtract_mean_in_groups(self):
    data = pd.DataFrame(
        data=[['0', 0.0, 1, 3.0],
              ['1', 0.0, 2, 2.0],
              ['1', 1.0, 3, 2.0],
              ['1', 1.0, 4, 1.0]],
        columns=['control_1', 'control_2', 'variable_1', 'variable_2'],
        index=['group1', 'group2', 'group3', 'group3'])
    expected_result = pd.DataFrame(
        data=[['0', 0.0, 2.5, 2.0],
              ['1', 0.0, 2.5, 2.0],
              ['1', 1.0, 2.0, 2.5],
              ['1', 1.0, 3.0, 1.5]],
        columns=data.columns,
        index=data.index).set_index(['control_1', 'control_2'], append=True)

    inference_data = data_preparation.InferenceData(data)
    result = inference_data.control_with_fixed_effect(
        strategy='quick',
        control_columns=['control_1', 'control_2'],
        min_frequency=1)

    pd.testing.assert_frame_equal(result, expected_result)

  def test_address_low_variance_removes_column(self):
    data = pd.DataFrame(
        data=[[0.0, 1.0, 0.0, 10.0],
              [0.0, 1.0, 0.0, 10.0],
              [1.0, 1.0, 0.0, 5.00],
              [1.0, 0.0, 0.0, 0.00]],
        columns=['control', 'variable', 'variable_1', 'outcome'])
    expected_result = pd.DataFrame(
        data=[[0.0, 1.0, 10.0],
              [0.0, 1.0, 10.0],
              [1.0, 1.0, 5.00],
              [1.0, 0.0, 0.00]],
        columns=['control', 'variable', 'outcome'])

    inference_data = data_preparation.InferenceData(
        data, target_column='outcome')
    result = inference_data.address_low_variance(drop=True)

    pd.testing.assert_frame_equal(result, expected_result)

  def test_address_collinearity_with_vif_removes_column(self):
    iris = datasets.load_iris()
    iris_data = pd.DataFrame(
        data=np.c_[iris['data'], iris['target']],
        columns=iris['feature_names'] + ['target'])
    expected_result = iris_data.drop(columns='petal length (cm)')

    inference_data = data_preparation.InferenceData(
        iris_data, target_column='target')
    result = inference_data.address_collinearity_with_vif(
        sequential=True,
        interactive=False,
        drop=True)

    pd.testing.assert_frame_equal(result, expected_result)

  def test_encode_categorical_covariate_dummy_variable_2(self):
    data = pd.DataFrame(
        data=[[0.0, 1.0, 'a', 10.0],
              [0.0, 1.0, 'b', 10.0],
              [1.0, 1.0, 'c', 5.00],
              [1.0, 0.0, 'a', 0.00]],
        columns=['control', 'variable_1', 'variable_2', 'outcome'])
    expected_result = pd.DataFrame(
        data=[[0.0, 1.0, 10.0, 1, 0, 0],
              [0.0, 1.0, 10.0, 0, 1, 0],
              [1.0, 1.0, 5.00, 0, 0, 1],
              [1.0, 0.0, 0.00, 1, 0, 0]],
        columns=[
            'control', 'variable_1', 'outcome', 'variable_2_a', 'variable_2_b',
            'variable_2_c'
        ])

    inference_data = data_preparation.InferenceData(
        data, target_column='outcome')
    result = inference_data.encode_categorical_covariates(
        columns=['variable_2'])

    pd.testing.assert_frame_equal(result, expected_result)

  @parameterized.named_parameters(
      ('single_selections', ['1', '2', '3'], ['1', '2', '3'], True),
      ('double_selection', ['1,2', '3'], ['1', '2', '3'], True),
      ('early_stopping', ['1', ''], ['1'], True),
      ('all_at_once', ['1,2,3'], ['1', '2', '3'], True),
      ('not_sequential', ['1,2'], ['1', '2'], False),
  )
  def test_address_collinearity_with_vif_interactive(
      self, user_inputs, expected_dropped, sequential):
    dataframe = pd.DataFrame(
        data=[[1.1, 2.1, 3.1, 4.1, 0],
              [1.0, 2.0, 3.0, 4.0, 0],
              [1.0, 2.0, 3.0, 4.0, 0],
              [1.0, 2.0, 3.0, 4.0, 1]],
        columns=['1', '2', '3', '4', 'target'])
    data = data_preparation.InferenceData(dataframe, target_column='target')

    with mock.patch.object(data_preparation, '_input_mock') as input_mock:
      # Avoid Colab\Notebook prints in tests output
      with mock.patch.object(data_preparation, '_print_mock') as _:
        user_inputs = list(reversed(user_inputs))
        input_mock.side_effect = lambda x: user_inputs.pop()

        result = data.address_collinearity_with_vif(
            sequential=sequential,
            interactive=True,
            drop=True)

    pd.testing.assert_frame_equal(
        result,
        dataframe.drop(expected_dropped, axis=1))


if __name__ == '__main__':
  unittest.main()
