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
"""Tests for gps_building_blocks.analysis.exp_design.ab_testing_design."""

from absl.testing import absltest
import numpy as np
from gps_building_blocks.analysis.exp_design import ab_testing_design

BASELINE_CONVERSION_RATE_PERCENTAGE = 5
BASELINE_AVERAGE_KPI = 5.5
BASELINE_STDEV_KPI = 3.5
EXPECTED_UPLIFT_PERCENTAGE = 10

BINARY_LABELS = np.array(
    [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
PROBABILITY_PREDICTIONS = np.array([
    0.7, 0.63, 0.4, 0.77, 0.45, 0.8, 0.41, 0.82, 0.7, 0.6, 0.5, 0.45, 0.74,
    0.11, 0.21, 0.05, 0.67, 0.79, 0.60, 0.10
])
NUMERIC_LABELS = np.array([
    60.54, 51.74, 31.52, 12.56, 42.53, 74.24, 55.36, 58.16, 39.93, 26.3, 95.5,
    31.98, 33.26, 40.15, 62.13, 54.33, 68.9, 34.09, 25.33, 79.46
])
NUMERIC_PREDICTIONS = np.array([
    65.55, 58.08, 32.97, 21.39, 37.98, 79.83, 63.19, 68.28, 48.45, 29.57, 89.17,
    36.91, 17.45, 46.88, 60.62, 55.53, 75.3, 43.0, 33.19, 82.34
])


class ABTestingExperimentalDesignTest(absltest.TestCase):

  def test_calc_chisquared_sample_size_returns_correct_values(self):
    result_sample_size = ab_testing_design.calc_chisquared_sample_size(
        baseline_conversion_rate_percentage=BASELINE_CONVERSION_RATE_PERCENTAGE,
        expected_uplift_percentage=EXPECTED_UPLIFT_PERCENTAGE)

    self.assertEqual(result_sample_size, 14913.0)

  def test_calc_chisquared_sample_size_change_power_and_confidence(self):
    result_sample_size = ab_testing_design.calc_chisquared_sample_size(
        baseline_conversion_rate_percentage=BASELINE_CONVERSION_RATE_PERCENTAGE,
        expected_uplift_percentage=EXPECTED_UPLIFT_PERCENTAGE,
        power_percentage=90,
        confidence_level_percentage=99)

    self.assertEqual(result_sample_size, 28271.0)

  def test_calc_chisquared_sample_sizes_for_bins_returns_correct_values(self):
    results = ab_testing_design.calc_chisquared_sample_sizes_for_bins(
        labels=BINARY_LABELS,
        probability_predictions=PROBABILITY_PREDICTIONS,
        number_bins=3,
        uplift_percentages=[10, 20],
        power_percentages=[80, 90],
        confidence_level_percentages=[90, 95])

    self.assertEqual(results.shape, (24, 8))
    self.assertListEqual(
        list(results.columns), [
            'bin_number', 'bin_size', 'min_probability', 'conv_rate_percentage',
            'uplift_percentage', 'power_percentage',
            'confidence_level_percentage', 'required_sample_size'
        ])
    self.assertListEqual(
        list(results['required_sample_size']), [
            106.0, 79.0, 62.0, 421.0, 343.0, 314.0, 248.0, 86.0, 526.0, 429.0,
            393.0, 310.0, 2102.0, 1571.0, 1237.0, 1714.0, 351.0, 286.0, 262.0,
            207.0, 1401.0, 1142.0, 1047.0, 825.0])

  def test_resulted_bin_metrics_does_not_contain_nas(self):
    results = ab_testing_design.calc_chisquared_sample_sizes_for_bins(
        labels=BINARY_LABELS,
        probability_predictions=PROBABILITY_PREDICTIONS,
        number_bins=3)

    self.assertFalse(results.isna().values.any())

  def test_calc_chisquared_sample_sizes_for_cumulative_bins_returns_right_vals(
      self):
    results = ab_testing_design.calc_chisquared_sample_sizes_for_cumulative_bins(
        labels=BINARY_LABELS,
        probability_predictions=PROBABILITY_PREDICTIONS,
        number_bins=5,
        uplift_percentages=[10, 20],
        power_percentages=[80, 90],
        confidence_level_percentages=[90, 95])

    self.assertEqual(results.shape, (40, 9))
    self.assertListEqual(
        list(results.columns), [
            'cumulative_bin_number', 'bin_size', 'bin_size_percentage',
            'min_probability', 'conv_rate_percentage', 'uplift_percentage',
            'power_percentage', 'confidence_level_percentage',
            'required_sample_size'
        ])
    self.assertListEqual(
        list(results['required_sample_size']), [
            207.0, 262.0, 286.0, 351.0, 52.0, 66.0, 72.0, 88.0, 371.0, 471.0,
            514.0, 631.0, 93.0, 118.0, 129.0, 158.0, 442.0, 561.0, 612.0, 751.0,
            111.0, 141.0, 153.0, 188.0, 371.0, 471.0, 514.0, 631.0, 93.0, 118.0,
            129.0, 158.0, 619.0, 785.0, 857.0, 1051.0, 155.0, 197.0, 215.0,
            263.0
        ])

  def test_calc_t_sample_size_returns_correct_values(self):
    result_sample_size = ab_testing_design.calc_t_sample_size(
        baseline_average=BASELINE_AVERAGE_KPI,
        baseline_stdev=BASELINE_STDEV_KPI,
        expected_uplift_percentage=EXPECTED_UPLIFT_PERCENTAGE)

    self.assertEqual(result_sample_size, 637)

  def test_calc_t_sample_sizes_for_bins_returns_correct_values(self):
    results = ab_testing_design.calc_t_sample_sizes_for_bins(
        labels=NUMERIC_LABELS,
        numeric_predictions=NUMERIC_PREDICTIONS,
        number_bins=3,
        uplift_percentages=[5, 7],
        power_percentages=[80, 90],
        confidence_level_percentages=[90, 95])

    self.assertEqual(results.shape, (24, 9))
    self.assertListEqual(
        list(results.columns),
        ['bin_number', 'bin_size', 'min_predicted_val', 'average_actual_val',
         'stdev_actual_val', 'uplift_percentage', 'power_percentage',
         'confidence_level_percentage', 'required_sample_size'])
    self.assertListEqual(
        list(results['required_sample_size']),
        [201, 255, 278, 342, 103, 131, 142, 175, 255, 323, 352, 433, 130, 165,
         180, 221, 496, 629, 686, 842, 253, 322, 351, 430])

  def test_calc_t_sample_sizes_for_cumulative_bins_returns_right_vals(
      self):
    results = ab_testing_design.calc_t_sample_sizes_for_cumulative_bins(
        labels=NUMERIC_LABELS,
        numeric_predictions=NUMERIC_PREDICTIONS,
        number_bins=3,
        uplift_percentages=[5, 7],
        power_percentages=[80, 90],
        confidence_level_percentages=[90, 95])

    self.assertEqual(results.shape, (24, 10))
    self.assertListEqual(
        list(results.columns),
        ['cumulative_bin_number', 'bin_size', 'bin_size_percentage',
         'min_predicted_val', 'mean_actual_val', 'stdev_actual_val',
         'uplift_percentage', 'power_percentage', 'confidence_level_percentage',
         'required_sample_size'
        ])
    self.assertListEqual(
        list(results['required_sample_size']),
        [70, 89, 97, 119, 36, 46, 50, 61, 227, 288, 314, 386, 116, 148, 161,
         197, 743, 944, 1029, 1263, 380, 482, 526, 645])


if __name__ == '__main__':
  absltest.main()
