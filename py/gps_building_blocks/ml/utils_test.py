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


class ModelDisgnosticsUtilsTest(absltest.TestCase):

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


if __name__ == '__main__':
  absltest.main()
