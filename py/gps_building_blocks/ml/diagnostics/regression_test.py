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
"""Tests for gps_building_blocks.ml.diagnostics.regression."""

from absl.testing import absltest
import pandas as pd
from gps_building_blocks.ml.diagnostics import regression

_TEST_DATA = pd.DataFrame({
    'label': [60.54, 51.74, 31.52, 12.56, 37.98, 74.24, 55.36, 58.16, 39.93,
              26.3, 89.17, 31.98, 10.61, 40.15, 60.62, 54.33, 68.9, 34.09,
              25.33, 79.46],
    'prediction': [65.55, 58.08, 32.97, 21.39, 42.53, 79.83, 63.19, 68.28,
                   48.45, 29.57, 95.5, 36.91, 17.45, 46.88, 62.13, 55.53, 75.3,
                   43.0, 33.19, 82.34]
})


class RegressionDiagnosticsTest(absltest.TestCase):

  def test_calc_performance_metrics_returns_correct_values(self):
    mean_squared_error = 39.7459
    root_mean_squared_error = 6.3044
    mean_squared_log_error = 0.1963
    mean_absolute_error = 5.755
    mean_absolute_percentage_error = 0.1794
    r_squared = 0.9108
    pearson_correlation = 0.9926

    results = regression.calc_performance_metrics(
        labels=_TEST_DATA['label'].values,
        predictions=_TEST_DATA['prediction'].values)

    self.assertAlmostEqual(mean_squared_error,
                           results.mean_squared_error)
    self.assertAlmostEqual(root_mean_squared_error,
                           results.root_mean_squared_error)
    self.assertAlmostEqual(mean_squared_log_error,
                           results.mean_squared_log_error)
    self.assertAlmostEqual(mean_absolute_error,
                           results.mean_absolute_error)
    self.assertAlmostEqual(mean_absolute_percentage_error,
                           results.mean_absolute_percentage_error)
    self.assertAlmostEqual(r_squared,
                           results.r_squared)
    self.assertAlmostEqual(pearson_correlation,
                           results.pearson_correlation)


if __name__ == '__main__':
  absltest.main()
