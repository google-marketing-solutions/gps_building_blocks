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
"""Tests for gps_building_blocks.ml.preprocessing.vif."""

import numpy as np
import pandas as pd
from sklearn import datasets

from absl.testing import absltest
from gps_building_blocks.ml.preprocessing import vif


class VifTest(absltest.TestCase):

  @classmethod
  def setUpClass(cls):
    super(VifTest, cls).setUpClass()
    iris = datasets.load_iris()
    iris_df = pd.DataFrame(
        data=np.c_[iris['data'], iris['target']],
        columns=iris['feature_names'] + ['target'])
    cls.data = iris_df
    cls.unsorted_vifs = [7.07, 2.10, 31.26, 16.09]
    cls.singular_correlation_matrix_df = pd.DataFrame(
        data=[[1.1, 2.1, 3.1, 4.1],
              [1.0, 2.0, 3.0, 4.0],
              [1.0, 2.0, 3.0, 4.0],
              [1.0, 2.0, 3.0, 4.0]],
        columns=['c1', 'c2', 'c3', 'c4'])
    cls.ill_conditioned_correlation_matrix_df = pd.DataFrame(
        data=[[1.0, 2.0, 3.0, 4.0],
              [0.0, 2.0, 0.0, 1.0],
              [1.0, 1.0, 2.0, 5.0],
              [0.0, 2.0, 3.0, 0.0]],
        columns=['c1', 'c2', 'c3', 'c4'])

  def test_calculate_vif_correct_results_inversion_method(self):
    expected = sorted(
        self.unsorted_vifs, reverse=True)  # by default it's sorted

    # Must drop response variable.
    vifs = vif.calculate_vif(
        self.data.drop(columns='target'), use_correlation_matrix_inversion=True)
    calculated_results = vifs['VIF'].round(2).to_list()

    self.assertListEqual(calculated_results, expected)

  def test_calculate_vif_correct_results_inversion_method_with_corr_matrix(
      self):
    # Must drop response variable.
    corr_matrix = self.data.drop(columns='target').corr()
    expected = sorted(
        self.unsorted_vifs, reverse=True)  # by default it's sorted

    vifs = vif.calculate_vif(
        self.data.drop(columns='target'),
        use_correlation_matrix_inversion=True,
        corr_matrix=corr_matrix)
    calculated_results = vifs['VIF'].round(2).to_list()

    self.assertListEqual(calculated_results, expected)

  def test_calculate_vif_correct_results_regression_method(self):
    # Must drop response variable.
    expected = sorted(
        self.unsorted_vifs, reverse=True)  # by default it's sorted

    vifs = vif.calculate_vif(
        self.data.drop(columns='target'),
        use_correlation_matrix_inversion=False)
    calculated_results = vifs['VIF'].round(2).to_list()

    self.assertListEqual(calculated_results, expected)

  def test_calculate_vif_sorted_flag_unsorted_results(self):
    vifs = vif.calculate_vif(self.data.drop(columns='target'), sort=False)
    calculated_results = vifs['VIF'].round(2).to_list()

    self.assertListEqual(calculated_results, self.unsorted_vifs)

  def test_inversion_method_throws_singular_error_on_singular_data(self):
    with self.assertRaises(vif.SingularDataError):
      vif.calculate_vif(
          self.singular_correlation_matrix_df,
          use_correlation_matrix_inversion=True)

  def test_regression_method_doesnt_throw_singular_error_on_singular_data(self):
    vifs = vif.calculate_vif(
        self.singular_correlation_matrix_df,
        use_correlation_matrix_inversion=False)

    self.assertNotEmpty(vifs)

  def test_inversion_method_throws_warning_on_ill_conditioned_data(self):
    with self.assertWarns(vif.IllConditionedDataWarning):
      vif.calculate_vif(
          self.ill_conditioned_correlation_matrix_df,
          use_correlation_matrix_inversion=True,
          raise_on_ill_conditioned=False)

  def test_inversion_method_throws_error_on_ill_conditioned_data(self):
    with self.assertRaises(vif.IllConditionedDataError):
      vif.calculate_vif(
          self.ill_conditioned_correlation_matrix_df,
          use_correlation_matrix_inversion=True,
          raise_on_ill_conditioned=True)

  def test_regression_method_doesnt_throw_errors_on_ill_conditioned_data(self):
    vifs = vif.calculate_vif(
        self.ill_conditioned_correlation_matrix_df,
        use_correlation_matrix_inversion=False,
        raise_on_ill_conditioned=True)

    self.assertNotEmpty(vifs)

if __name__ == '__main__':
  absltest.main()
