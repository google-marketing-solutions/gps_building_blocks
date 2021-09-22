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
from scipy import stats
from sklearn import datasets
from sklearn import model_selection

from absl.testing import absltest
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

  def test_vif_raises_error_on_singular_correlation_matrix(self):
    singular_correlation_matrix_df = pd.DataFrame(
        data=[[1.1, 2.1, 3.1, 4.1, 1.0],
              [1.0, 2.0, 3.0, 4.0, 1.0],
              [1.0, 2.0, 3.0, 4.0, 1.0],
              [1.0, 2.0, 3.0, 4.0, 1.0]],
        columns=['control', 'variable_1', 'variable_2', 'variable_3',
                 'outcome'])
    inference_data = data_preparation.InferenceData(
        singular_correlation_matrix_df, target_column='outcome')

    with self.assertRaises(data_preparation.SingularDataError):
      inference_data.address_collinearity_with_vif(
          handle_singular_data_errors_automatically=False)

  def test_vif_raises_error_on_ill_conditioned_correlation_matrix(self):
    ill_conditioned_correlation_matrix_df = pd.DataFrame(
        data=[[1.0, 2.0, 3.0, 4.0, 1.0],
              [0.0, 2.0, 0.0, 1.0, 1.0],
              [1.0, 1.0, 2.0, 5.0, 1.0],
              [0.0, 2.0, 3.0, 0.0, 1.0]],
        columns=['control', 'variable_1', 'variable_2', 'variable_3',
                 'outcome'])
    inference_data = data_preparation.InferenceData(
        ill_conditioned_correlation_matrix_df, target_column='outcome')

    with self.assertRaises(data_preparation.SingularDataError):
      inference_data.address_collinearity_with_vif(
          handle_singular_data_errors_automatically=False)

  def test_vif_error_has_correct_message(self):
    ill_conditioned_correlation_matrix_df = pd.DataFrame(
        data=[[1.0, 2.0, 3.0, 4.0, 1.0],
              [0.0, 2.0, 0.0, 1.0, 1.0],
              [1.0, 1.0, 2.0, 5.0, 1.0],
              [0.0, 2.0, 3.0, 0.0, 1.0]],
        columns=['control', 'variable_1', 'variable_2', 'variable_3',
                 'outcome'])
    inference_data = data_preparation.InferenceData(
        ill_conditioned_correlation_matrix_df, target_column='outcome')

    expected_message = (
        'Inference Data has a singular or nearly singular correlation matrix. '
        'This could be caused by extremely correlated or collinear columns. '
        'The three pairs of columns with the highest absolute correlation '
        'coefficients are: (control,variable_3): 0.970, (variable_1,variable_3)'
        ': -0.700, (control,variable_1): -0.577. This could also be caused by '
        'columns with extremiely low variance. Recommend running the '
        'address_low_variance() method before VIF. Alternatively, consider '
        'running address_collinearity_with_vif() with '
        'use_correlation_matrix_inversion=False to avoid this error.'
    )
    with self.assertRaises(
        data_preparation.SingularDataError, msg=expected_message):
      inference_data.address_collinearity_with_vif(
          handle_singular_data_errors_automatically=False)

  def test_vif_noise_injection_catches_perfect_correlation(self):
    iris = datasets.load_iris()
    iris_data = pd.DataFrame(
        data=np.c_[iris['data'], iris['target']],
        columns=iris['feature_names'] + ['target'])
    iris_data['perfectly_correlated_column'] = iris_data['petal length (cm)']
    expected_result = iris_data.drop(
        columns=['petal length (cm)', 'perfectly_correlated_column'])

    inference_data = data_preparation.InferenceData(
        iris_data, target_column='target')

    result = inference_data.address_collinearity_with_vif(
        vif_method='quick',
        drop=True,
        handle_singular_data_errors_automatically=True,
        vif_threshold=50.0)

    pd.testing.assert_frame_equal(result, expected_result)

  def test_vif_noise_injection_catches_perfect_collinearity(self):
    iris = datasets.load_iris()
    iris_data = pd.DataFrame(
        data=np.c_[iris['data'], iris['target']],
        columns=iris['feature_names'] + ['target'])
    iris_data['perfectly_collinear_column'] = iris_data[
        'petal length (cm)'] + iris_data['petal width (cm)']
    expected_result = iris_data.drop(columns=[
        'petal length (cm)', 'petal width (cm)', 'perfectly_collinear_column'
    ])

    inference_data = data_preparation.InferenceData(
        iris_data, target_column='target')

    result = inference_data.address_collinearity_with_vif(
        vif_method='quick',
        drop=True,
        handle_singular_data_errors_automatically=True,
        vif_threshold=50.0)

    pd.testing.assert_frame_equal(result, expected_result)

  def test_vif_noise_injection_fails_correctly_when_too_few_samples(self):
    too_few_samples_df = pd.DataFrame(
        data=[[1.0, 2.0, 3.0, 4.0, 1.0],
              [0.0, 2.0, 0.0, 1.0, 1.0],
              [1.0, 1.0, 2.0, 5.0, 1.0]],
        columns=['control', 'variable_1', 'variable_2', 'variable_3',
                 'outcome'])
    inference_data = data_preparation.InferenceData(
        too_few_samples_df, target_column='outcome')

    expected_regex = (
        'Automatic attempt to resolve SingularDataError by '
        'injecting artifical noise to the data has failed. This '
        'probably means the dataset has too many features relative '
        'to the number of samples.')
    with self.assertRaisesRegex(data_preparation.SingularDataError,
                                expected_regex):
      inference_data.address_collinearity_with_vif(
          handle_singular_data_errors_automatically=True)

  def test_vif_method_fails_correctly_with_unknown_value(self):
    inference_data = data_preparation.InferenceData(self._missing_data)
    with self.assertRaises(ValueError):
      inference_data.address_collinearity_with_vif(
          vif_method='incorrect_value')

  @parameterized.named_parameters({
      'testcase_name': 'scale_10',
      'scaling': 10,
  }, {
      'testcase_name': 'scale_50',
      'scaling': 50,
  }, {
      'testcase_name': 'scale_-50',
      'scaling': -50,
  })
  def test_minmaxscaling_drops_appropriate_variables(self, scaling):
    data = pd.DataFrame(
        data=[[0.0, 1.0, 0.0, 10.0], [-0.5, 1.0, 0.0, 10.0],
              [0.1, 1.0, 0.0, 5.00], [0.2, 0.0, 0.0, 0.00]],
        columns=['variable_0', 'variable_1', 'variable_2', 'outcome'])
    data = data * scaling
    expected_result = data[['variable_1', 'outcome']]

    inference_data = data_preparation.InferenceData(
        data)
    result = inference_data.address_low_variance(
        threshold=.15,
        drop=True,
        minmax_scaling=True,
    )

    pd.testing.assert_frame_equal(result, expected_result)

  def test_zscored_input_raises_warning(self):
    data = pd.DataFrame(
        data=[[0.0, 1.0, 0.0, 10.0], [-0.5, 1.0, 0.0, 10.0],
              [0.1, 1.0, 0.0, 5.00], [0.2, 0.0, 0.0, 0.00]],
        columns=['variable_0', 'variable_1', 'variable_2', 'variable_3'])

    data = data.apply(stats.zscore).fillna(0)
    inference_data = data_preparation.InferenceData(data)
    with self.assertWarns(Warning):
      _ = inference_data.address_low_variance()

  def test_minmaxscaling_with_invalid_threshold_raises_warning(self):
    data = pd.DataFrame(
        data=[[0.0, 1.0, 0.0, 10.0], [-0.5, 1.0, 0.0, 10.0],
              [0.1, 1.0, 0.0, 5.00], [0.2, 0.0, 0.0, 0.00]],
        columns=['variable_0', 'variable_1', 'variable_2', 'variable_3'])

    inference_data = data_preparation.InferenceData(data)
    with self.assertWarns(Warning):
      _ = inference_data.address_low_variance(minmax_scaling=True, threshold=.5)

  def test_address_collinearity_with_vif_removes_column(self):
    iris = datasets.load_iris()
    iris_data = pd.DataFrame(
        data=np.c_[iris['data'], iris['target']],
        columns=iris['feature_names'] + ['target'])
    expected_result = iris_data.drop(columns='petal length (cm)')

    inference_data = data_preparation.InferenceData(
        iris_data, target_column='target')
    result = inference_data.address_collinearity_with_vif(
        vif_method='sequential',
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
      ('single_selections', ['1', '2', '3'], ['1', '2', '3']),
      ('double_selection', ['1,2', '3'], ['1', '2', '3']),
      ('early_stopping', ['1', ''], ['1']),
      ('all_at_once', ['1,2,3'], ['1', '2', '3']),
  )
  def test_address_collinearity_with_vif_interactive(
      self, user_inputs, expected_dropped):
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
            vif_method='interactive',
            drop=True,
            use_correlation_matrix_inversion=False
            )

    pd.testing.assert_frame_equal(
        result,
        dataframe.drop(expected_dropped, axis=1))

  @parameterized.named_parameters(
      ('onehot_returns_expected_bins', False, False, pd.DataFrame(
          [[1, 0, 0, 0, 0],
           [1, 0, 0, 0, 0],
           [1, 0, 0, 0, 0],
           [1, 0, 0, 0, 0],
           [1, 0, 0, 0, 0],
           [0, 1, 0, 0, 0],
           [0, 1, 0, 0, 0],
           [0, 1, 0, 0, 0],
           [0, 1, 0, 0, 0],
           [0, 0, 1, 0, 0],
           [0, 0, 0, 0, 1]],
          columns=['variable_(-0.02, 4.0]', 'variable_(4.0, 8.0]',
                   'variable_(8.0, 12.0]', 'variable_(12.0, 16.0]',
                   'variable_(16.0, 20.0]'])),
      ('equal_sized_onehot_returns_expected_bins', True, False, pd.DataFrame(
          [[1, 0, 0, 0, 0],
           [1, 0, 0, 0, 0],
           [1, 0, 0, 0, 0],
           [0, 1, 0, 0, 0],
           [0, 1, 0, 0, 0],
           [0, 0, 1, 0, 0],
           [0, 0, 1, 0, 0],
           [0, 0, 0, 1, 0],
           [0, 0, 0, 1, 0],
           [0, 0, 0, 0, 1],
           [0, 0, 0, 0, 1]],
          columns=['variable_(-0.001, 2.0]', 'variable_(2.0, 4.0]',
                   'variable_(4.0, 6.0]', 'variable_(6.0, 8.0]',
                   'variable_(8.0, 20.0]'])),
      ('scalar_numeric_returns_expected_bins', False, True, pd.DataFrame(
          [0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 4], columns=['variable'])),
      ('equal_sized_numeric_expected_bins', True, True, pd.DataFrame(
          [0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4], columns=['variable'])),
  )
  def test_descretize(self, equal_sized_bins, numeric, expected_result):
    data = data_preparation.InferenceData(pd.DataFrame(
        data=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 20],
        columns=['variable']))

    result = data.discretize_numeric_covariate(
        'variable', equal_sized_bins=equal_sized_bins, bins=5, numeric=numeric)

    pd.testing.assert_frame_equal(result, expected_result, check_dtype=False)

  @parameterized.named_parameters(
      ('with_groups_kfold_as_int',
       3,
       np.array([0, 0, 1, 1, 2, 2, 3, 3, 3, 3]),
       [pd.DataFrame({'variable': [0, 1, 2, 3, 4, 5]}),
        pd.DataFrame({'variable': [2, 3, 6, 7, 8, 9]},
                     index=[2, 3, 6, 7, 8, 9]),
        pd.DataFrame({'variable': [0, 1, 4, 5, 6, 7, 8, 9]},
                     index=[0, 1, 4, 5, 6, 7, 8, 9])],
       [pd.DataFrame({'variable': [6, 7, 8, 9]}, index=[6, 7, 8, 9]),
        pd.DataFrame({'variable': [0, 1, 4, 5]}, index=[0, 1, 4, 5]),
        pd.DataFrame({'variable': [2, 3]}, index=[2, 3])]),
      ('with_groups_kfold_as_object',
       model_selection.GroupKFold(n_splits=3),
       np.array([0, 0, 1, 1, 2, 2, 3, 3, 3, 3]),
       [pd.DataFrame({'variable': [0, 1, 2, 3, 4, 5]}),
        pd.DataFrame({'variable': [2, 3, 6, 7, 8, 9]},
                     index=[2, 3, 6, 7, 8, 9]),
        pd.DataFrame({'variable': [0, 1, 4, 5, 6, 7, 8, 9]},
                     index=[0, 1, 4, 5, 6, 7, 8, 9])],
       [pd.DataFrame({'variable': [6, 7, 8, 9]}, index=[6, 7, 8, 9]),
        pd.DataFrame({'variable': [0, 1, 4, 5]}, index=[0, 1, 4, 5]),
        pd.DataFrame({'variable': [2, 3]}, index=[2, 3])]),
  )
  def test_split_with_groups_yields_expected_folds_with_non_overlaping_groups(
      self,
      cross_validation,
      groups,
      expected_trains,
      expected_tests):
    data = data_preparation.InferenceData(
        pd.DataFrame({
            'variable': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        }))

    iterator = zip(data.split(cross_validation=cross_validation, groups=groups),
                   expected_trains,
                   expected_tests)
    for (train_data, test_data), expected_train, expected_test in iterator:
      train_groups = set(groups[train_data.data.index.tolist()])
      test_groups = set(groups[test_data.data.index.tolist()])

      pd.testing.assert_frame_equal(
          train_data.data, expected_train, check_dtype=False)
      pd.testing.assert_frame_equal(
          test_data.data, expected_test, check_dtype=False)
      self.assertEmpty(train_groups.intersection(test_groups))

  @parameterized.named_parameters(
      ('without_groups_kfold_as_int', 3,
       [pd.DataFrame({'variable': [4, 5, 6, 7, 8, 9]},
                     index=[4, 5, 6, 7, 8, 9]),
        pd.DataFrame({'variable': [0, 1, 2, 3, 7, 8, 9]},
                     index=[0, 1, 2, 3, 7, 8, 9]),
        pd.DataFrame({'variable': [0, 1, 2, 3, 4, 5, 6]},
                     index=[0, 1, 2, 3, 4, 5, 6])],
       [pd.DataFrame({'variable': [0, 1, 2, 3]}, index=[0, 1, 2, 3]),
        pd.DataFrame({'variable': [4, 5, 6]}, index=[4, 5, 6]),
        pd.DataFrame({'variable': [7, 8, 9]}, index=[7, 8, 9])]),
      ('without_groups_kfold_as_object',
       model_selection.KFold(n_splits=3),
       [pd.DataFrame({'variable': [4, 5, 6, 7, 8, 9]},
                     index=[4, 5, 6, 7, 8, 9]),
        pd.DataFrame({'variable': [0, 1, 2, 3, 7, 8, 9]},
                     index=[0, 1, 2, 3, 7, 8, 9]),
        pd.DataFrame({'variable': [0, 1, 2, 3, 4, 5, 6]},
                     index=[0, 1, 2, 3, 4, 5, 6])],
       [pd.DataFrame({'variable': [0, 1, 2, 3]}, index=[0, 1, 2, 3]),
        pd.DataFrame({'variable': [4, 5, 6]}, index=[4, 5, 6]),
        pd.DataFrame({'variable': [7, 8, 9]}, index=[7, 8, 9])]),
  )
  def test_split_without_groups_yields_expected_folds(self,
                                                      cross_validation,
                                                      expected_trains,
                                                      expected_tests):
    data = data_preparation.InferenceData(
        pd.DataFrame({
            'variable': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        }))

    iterator = zip(data.split(cross_validation=cross_validation),
                   expected_trains,
                   expected_tests)
    for (train_data, test_data), expected_train, expected_test in iterator:
      pd.testing.assert_frame_equal(
          train_data.data, expected_train, check_dtype=False)
      pd.testing.assert_frame_equal(
          test_data.data, expected_test, check_dtype=False)

  def test_split_can_use_non_integer_indices(self):
    expected_trains = [
        pd.DataFrame(data={'variable': [4, 5, 6, 7, 8, 9]},
                     index=['4', '5', '6', '7', '8', '9']),
        pd.DataFrame(data={'variable': [0, 1, 2, 3, 7, 8, 9]},
                     index=['0', '1', '2', '3', '7', '8', '9']),
        pd.DataFrame(data={'variable': [0, 1, 2, 3, 4, 5, 6]},
                     index=['0', '1', '2', '3', '4', '5', '6'])]
    expected_tests = [
        pd.DataFrame({'variable': [0, 1, 2, 3]}, index=['0', '1', '2', '3']),
        pd.DataFrame({'variable': [4, 5, 6]}, index=['4', '5', '6']),
        pd.DataFrame({'variable': [7, 8, 9]}, index=['7', '8', '9'])]
    data = data_preparation.InferenceData(
        pd.DataFrame(data={'variable': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],},
                     index=['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']))

    iterator = zip(data.split(cross_validation=3),
                   expected_trains,
                   expected_tests)
    for (train_data, test_data), expected_train, expected_test in iterator:
      pd.testing.assert_frame_equal(
          train_data.data, expected_train, check_dtype=False)
      pd.testing.assert_frame_equal(
          test_data.data, expected_test, check_dtype=False)


if __name__ == '__main__':
  absltest.main()
