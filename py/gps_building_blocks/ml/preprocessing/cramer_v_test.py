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

"""Tests for gps_building_blocks.ml.preprocessing.cramer_v."""

import numpy as np
from gps_building_blocks.ml.preprocessing import cramer_v
from absl.testing import absltest


class CramerVTest(absltest.TestCase):

  def test_cramer_returns_expected_value(self):
    x1 = np.array([1, 2, 1])
    x2 = np.array([2, 1, 1])
    cramer = cramer_v.cramer_v(x1, x2)
    self.assertEqual(cramer, 0.25)

  def test_cramer_returns_one(self):
    x1 = np.array(['a'] * 98 + ['b'] + ['c'])
    cramer = cramer_v.cramer_v(x1, x1)
    self.assertEqual(cramer, 1)

  def test_cramer_raises_assertion_error_for_non_categorical_input(self):
    x1 = np.arange(100)
    x2 = np.arange(100) + 1
    self.assertRaises(AssertionError, cramer_v.cramer_v, x1, x2)

  def test_cramer_swapped_variables_return_same_value(self):
    x1 = np.array([1, 2, 1])
    x2 = np.array([1, 2, 2])
    cramer_1_2 = cramer_v.cramer_v(x1, x2)
    cramer_2_1 = cramer_v.cramer_v(x2, x1)
    self.assertEqual(cramer_1_2, cramer_2_1)


if __name__ == '__main__':
  absltest.main()
