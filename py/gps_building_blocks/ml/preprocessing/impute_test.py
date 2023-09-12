# Copyright 2022 Google LLC
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
"""Tests for impute module."""

import numpy as np
import pandas as pd
from pandas import testing
from sklearn import linear_model
from sklearn.experimental import enable_iterative_imputer  # pylint:disable=unused-import
from sklearn.impute import IterativeImputer

from absl.testing import absltest
from absl.testing import parameterized
from gps_building_blocks.ml.preprocessing import impute

data_dict = {
    'cat': list('abcdefg') + [np.nan],
    'num': [1, 2, 3, 4, 5, np.nan, 7, 8],
    'binary': [0, 0, np.nan, 1, 1, 1, 1, 0],
}
_MOCK_DATA = pd.DataFrame(data_dict)
_expected_data_types = ['categorical', 'numerical', 'binary']


class ImputeTest(parameterized.TestCase):

  def test_detects_expected_data_types(self):
    detected_data_types = impute.detect_data_types(
        _MOCK_DATA, categorical_cutoff=3
    )

    self.assertEqual(_expected_data_types, detected_data_types)

  def test_length_data_types_matches_data(self):
    detected_data_types = impute.detect_data_types(
        _MOCK_DATA, categorical_cutoff=3
    )

    self.assertLen(detected_data_types, len(_MOCK_DATA.columns))

  def test_numerical_data_remain_same_after_encoding(self):
    encoded_data, _ = impute.encode_categorical_data(
        _MOCK_DATA, _expected_data_types
    )

    testing.assert_frame_equal(
        _MOCK_DATA[['num', 'binary']], encoded_data[['num', 'binary']]
    )

  def test_data_remains_unchanged_if_no_missings(self):
    data_no_missing = _MOCK_DATA.dropna()
    detected_data_types = impute.detect_data_types(
        _MOCK_DATA, categorical_cutoff=3
    )
    encoded_data, _ = impute.encode_categorical_data(
        data_no_missing, detected_data_types
    )
    data_imputed = encoded_data.copy()
    data_imputed['cat'], _ = impute.impute_categorical_data(
        encoded_data, encoded_data['cat'], detected_data_types, random_state=0
    )
    data_imputed, _ = impute.impute_numerical_data(
        data=data_imputed,
        data_types=detected_data_types,
        imputer=IterativeImputer(),
    )
    testing.assert_frame_equal(encoded_data, data_imputed)

  def test_no_nans_after_imputation(self):
    encoded_data, _ = impute.encode_categorical_data(
        _MOCK_DATA, _expected_data_types
    )
    data_imputed = encoded_data.copy()
    data_imputed['cat'], _ = impute.impute_categorical_data(
        encoded_data, encoded_data['cat'], _expected_data_types, random_state=0
    )
    data_imputed, _ = impute.impute_numerical_data(
        data=data_imputed,
        data_types=_expected_data_types,
        imputer=IterativeImputer(),
    )
    sum_nans = data_imputed.isna().sum().sum()
    self.assertEqual(0, sum_nans)

  def test_ValueError_if_nans_in_categorical(self):
    detected_data_types = impute.detect_data_types(
        _MOCK_DATA, categorical_cutoff=3
    )
    data_imputed = _MOCK_DATA.copy()
    with self.assertRaises(ValueError):
      data_imputed, _ = impute.impute_numerical_data(
          data=data_imputed,
          data_types=detected_data_types,
          imputer=IterativeImputer(),
      )

  def test_ValueError_if_nonsupported_data_type(self):
    with self.assertRaises(ValueError):
      impute._retrieve_categorical_and_numerical_or_binary_columns(
          _MOCK_DATA, data_types=['categorical', 'numerical', 'date']
      )

  def test_no_overlap_between_numerical_and_nonnumerical_columns(self):
    categorical_columns, numerical_columns = (
        impute._retrieve_categorical_and_numerical_or_binary_columns(
            _MOCK_DATA, data_types=['categorical', 'numerical', 'binary']
        )
    )
    self.assertTrue(set(categorical_columns).isdisjoint(numerical_columns))

  def test_ValueError_if_passed_datatypes_dont_match_columns(self):
    with self.assertRaises(ValueError):
      _ = impute.run_imputation_pipeline(
          data=_MOCK_DATA,
          categorical_cutoff=3,
          data_types=['binary'],
          random_state=0,
      )

  def test_ValueError_if_missing_rate_outside_of_range(self):
    with self.assertRaises(ValueError):
      _ = impute.simulate_mixed_data_with_missings(rate_missings=-1)

  @parameterized.named_parameters(
      ('100_2_2_0', 100, 2, 2, 0),
      ('1_20_0_10', 1, 20, 0, 10),
      ('100_3_3_3', 100, 3, 3, 3),
  )
  def test_size_of_data_matches_inputs(
      self, n_samples, n_binary, n_categorical, n_continuous
  ):
    expected_columns = n_binary + n_categorical + n_continuous
    simulated_data, _ = impute.simulate_mixed_data_with_missings(
        n_samples, n_categorical, n_continuous, n_binary
    )

    simulated_rows, simulated_columns = np.shape(simulated_data)
    self.assertEqual(simulated_rows, n_samples)
    self.assertEqual(simulated_columns, expected_columns)

  def test_ValueError_if_not_enough_data_for_performance_gain_assessment(self):
    data_missing_dict = {
        'target': [1, 0, 1, 0],
        'num': [1, np.nan, np.nan, 5],
        'binary': [0, np.nan, 0, np.nan],
    }
    data_missing = pd.DataFrame(data_missing_dict)

    data_no_missing_dict = {
        'target': [1, 0, 1, 0],
        'num': [1, 5, 2, 4],
        'binary': [0, 1, 0, 1],
    }
    data_no_missing = pd.DataFrame(data_no_missing_dict)

    with self.assertRaisesRegex(ValueError, 'Not enough observation'):
      impute.calculate_model_performance_gain_from_imputation(
          model=linear_model.LogisticRegression(),
          data_imputed=data_no_missing,
          data_missings=data_missing,
          target=data_missing['target'],
          crossvalidation_folds=2,
      )

  def test_ValueError_if_non_numerical_data_for_model_gain_assessment(self):
    with self.assertRaisesRegex(ValueError, 'numerical'):
      impute.calculate_model_performance_gain_from_imputation(
          model=linear_model.LogisticRegression(),
          data_imputed=_MOCK_DATA,
          data_missings=_MOCK_DATA,
          target='cat',
      )

  def test_ValueError_if_no_missing_data_in_mae_calculation(self):
    with self.assertRaises(ValueError):
      impute.get_imputation_mean_absolute_error(
          data_ground_truth=_MOCK_DATA.fillna(0),
          data_missing=_MOCK_DATA.fillna(0),
          data_imputed=_MOCK_DATA.fillna(0),
          data_types=_expected_data_types,
      )


if __name__ == '__main__':
  absltest.main()
