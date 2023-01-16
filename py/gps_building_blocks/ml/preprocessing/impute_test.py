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
from sklearn.experimental import enable_iterative_imputer  # pylint:disable=unused-import
from sklearn.impute import IterativeImputer

from absl.testing import absltest
from gps_building_blocks.ml.preprocessing import impute

data_dict = {
    'cat': list('abcdefg') + [np.nan],
    'num': [1, 2, 3, 4, 5, np.nan, 7, 8],
    'binary': [0, 0, np.nan, 1, 1, 1, 1, 0]
}
_MOCK_DATA = pd.DataFrame(data_dict)
_expected_data_types = ['categorical', 'numerical', 'binary']


class ImputeTest(absltest.TestCase):

  def test_detects_expected_data_types(self):
    detected_data_types = impute.detect_data_types(
        _MOCK_DATA, categorical_cutoff=3)

    self.assertEqual(_expected_data_types, detected_data_types)

  def test_length_data_types_matches_data(self):
    detected_data_types = impute.detect_data_types(
        _MOCK_DATA, categorical_cutoff=3)

    self.assertLen(detected_data_types, len(_MOCK_DATA.columns))

  def test_numerical_data_remain_same_after_encoding(self):
    encoded_data, _ = impute.encode_categorical_data(_MOCK_DATA,
                                                     _expected_data_types)

    testing.assert_frame_equal(
        _MOCK_DATA[['num', 'binary']], encoded_data[['num', 'binary']]
    )

  def test_data_remains_unchanged_if_no_missings(self):
    data_no_missing = _MOCK_DATA.dropna()
    detected_data_types = impute.detect_data_types(
        _MOCK_DATA, categorical_cutoff=3)
    encoded_data, _ = impute.encode_categorical_data(data_no_missing,
                                                     detected_data_types)
    data_imputed = encoded_data.copy()
    data_imputed['cat'], _ = impute.impute_categorical_data(
        encoded_data, encoded_data['cat'], detected_data_types, random_state=0
    )
    data_imputed = impute.impute_numerical_data(data_imputed,
                                                detected_data_types,
                                                IterativeImputer())
    testing.assert_frame_equal(encoded_data, data_imputed)

  def test_no_nans_after_imputation(self):
    encoded_data, _ = impute.encode_categorical_data(_MOCK_DATA,
                                                     _expected_data_types)
    data_imputed = encoded_data.copy()
    data_imputed['cat'], _ = impute.impute_categorical_data(
        encoded_data, encoded_data['cat'], _expected_data_types, random_state=0
    )
    data_imputed, _ = impute.impute_numerical_data(data_imputed,
                                                   _expected_data_types,
                                                   IterativeImputer())
    sum_nans = data_imputed.isna().sum().sum()
    self.assertEqual(0, sum_nans)

  def test_ValueError_if_nans_in_categorical(self):
    detected_data_types = impute.detect_data_types(
        _MOCK_DATA, categorical_cutoff=3)
    data_imputed = _MOCK_DATA.copy()
    with self.assertRaises(ValueError):
      data_imputed, _ = impute.impute_numerical_data(
          data_imputed, detected_data_types, IterativeImputer()
      )

  def test_LGBM_raises_warning_for_using_categorical_featues(self):
    detected_data_types = impute.detect_data_types(
        _MOCK_DATA, categorical_cutoff=3)
    data_imputed = _MOCK_DATA.copy()
    with self.assertWarns(UserWarning):
      data_imputed['cat'], _ = impute.impute_categorical_data(
          data_imputed, data_imputed['cat'], detected_data_types, random_state=0
      )

  def test_ValueError_if_nonsupported_data_type(self):
    with self.assertRaises(ValueError):
      impute._get_categorical_and_numerical_or_binary_columns(
          _MOCK_DATA, data_types=['categorical', 'numerical', 'date']
      )

  def test_no_overlap_between_numerical_and_nonnumerical_columns(self):
    categorical_columns, numerical_columns = (
        impute._get_categorical_and_numerical_or_binary_columns(
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


if __name__ == '__main__':
  absltest.main()
