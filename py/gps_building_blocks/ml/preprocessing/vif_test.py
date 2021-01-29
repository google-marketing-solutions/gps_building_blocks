# Lint as: python3
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

  def test_calculate_vif_correct_results(self):
    # Must drop response variable.
    vifs = vif.calculate_vif(self.data.drop(columns='target'))
    calculated_results = vifs['VIF'].round(2).to_list()

    expected = sorted(self.unsorted_vifs, reverse=True)  # by default its sorted
    self.assertListEqual(calculated_results, expected)

  def test_calculate_vif_sorted_flag_unsorted_results(self):
    vifs = vif.calculate_vif(self.data.drop(columns='target'), sort=False)
    calculated_results = vifs['VIF'].round(2).to_list()

    self.assertListEqual(calculated_results, self.unsorted_vifs)


if __name__ == '__main__':
  absltest.main()
