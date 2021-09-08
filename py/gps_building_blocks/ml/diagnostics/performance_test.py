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
"""Tests for gps_building_blocks.ml.diagnostics.performance."""
import pandas as pd

from gps_building_blocks.ml.diagnostics import performance
from absl.testing import absltest
from absl.testing import parameterized


class ModelDiagnosticTest(parameterized.TestCase):

  @parameterized.named_parameters(
      ('3x3_eye', [[1, 0, 0], [0, 1, 0], [0, 0, 1]], 1.0, 1.0),
      ('3x3_only_one', [[0, 0, 0], [0, 0, 0], [1, 0, 0]], 0.0, 1.0),
      ('3x3_half', [[0, 0, 1], [0, 1, 0], [1, 0, 0]], 1 / 3, 1.0),
      ('3x3_worst', [[0, 0, 1], [1, 0, 0], [1, 0, 0]], 1 / 3, 1.0),
      ('4x4_eye', [[1, 0, 0, 0], [0, 1, 0, 0], [0, 0, 1, 0], [0, 0, 0, 1]],
       1.0, 1.0),
      ('4x4_middle', [[0, 0, 0, 1], [0, 0, 1, 0], [0, 1, 0, 0], [1, 0, 0, 0]],
       0.5, 0.5),
      ('4x4_worst', [[0, 0, 0, 1], [0, 0, 0, 1], [1, 0, 0, 0], [1, 0, 0, 0]],
       0.0, 0.5),
      ('5x5_eye', [[1, 0, 0, 0, 0], [0, 1, 0, 0, 0], [0, 0, 1, 0, 0],
                   [0, 0, 0, 1, 0], [0, 0, 0, 0, 1]], 1.0, 1.0),
      ('5x5_none', [[0, 0, 0, 0, 1], [0, 0, 0, 0, 1], [0, 0, 0, 0, 0],
                    [1, 0, 0, 0, 0], [1, 0, 0, 0, 0]], 0.0, 0.0),
  )
  def test_compute_quantile_accuracies(
      self, heatmap, expected_accuracy_1st, expected_accuracy_2nd):
    accuracy_1st, accuracy_2nd = performance._compute_quantile_accuracies(
        pd.DataFrame(heatmap))

    self.assertEqual(accuracy_1st, expected_accuracy_1st)
    self.assertEqual(accuracy_2nd, expected_accuracy_2nd)


if __name__ == '__main__':
  absltest.main()
