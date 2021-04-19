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
EXPECTED_UPLIFT_PERCENTAGE = 10
LABELS = np.array([1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
PREDICTIONS = np.array([
    0.7, 0.63, 0.4, 0.77, 0.45, 0.8, 0.41, 0.82, 0.7, 0.6, 0.5, 0.45, 0.74,
    0.11, 0.21, 0.05, 0.67, 0.79, 0.60, 0.10
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
        labels=LABELS, probability_predictions=PREDICTIONS, number_bins=3)

    self.assertEqual(results.shape, (24, 7))
    self.assertListEqual(
        list(results.columns), [
            'bin_number', 'bin_size', 'conv_rate_percentage',
            'uplift_percentage', 'power_percentage',
            'confidence_level_percentage', 'sample_size'
        ])
    self.assertListEqual(
        list(results['sample_size']), [
            248.0, 314.0, 343.0, 421.0, 62.0, 79.0, 86.0, 106.0, 928.0, 1178.0,
            1285.0, 1577.0, 232.0, 295.0, 322.0, 395.0, 1031.0, 1309.0, 1428.0,
            1752.0, 258.0, 328.0, 357.0, 438.0
        ])

  def test_resulted_bin_metrics_does_not_contain_nas(self):
    results = ab_testing_design.calc_chisquared_sample_sizes_for_bins(
        labels=LABELS, probability_predictions=PREDICTIONS, number_bins=3)

    self.assertFalse(results.isna().values.any())

  def test_calc_chisquared_sample_sizes_for_cumulative_bins_returns_right_vals(
      self):
    results = ab_testing_design.calc_chisquared_sample_sizes_for_cumulative_bins(
        labels=LABELS, probability_predictions=PREDICTIONS, number_bins=5)

    self.assertEqual(results.shape, (40, 8))
    self.assertListEqual(
        list(results.columns), [
            'cumulative_bin_number', 'bin_size', 'bin_size_percentage',
            'conv_rate_percentage', 'uplift_percentage', 'power_percentage',
            'confidence_level_percentage', 'sample_size'
        ])
    self.assertListEqual(
        list(results['sample_size']), [
            207.0, 262.0, 286.0, 351.0, 52.0, 66.0, 72.0, 88.0, 371.0, 471.0,
            514.0, 631.0, 93.0, 118.0, 129.0, 158.0, 442.0, 561.0, 612.0, 751.0,
            111.0, 141.0, 153.0, 188.0, 371.0, 471.0, 514.0, 631.0, 93.0, 118.0,
            129.0, 158.0, 619.0, 785.0, 857.0, 1051.0, 155.0, 197.0, 215.0,
            263.0
        ])


if __name__ == '__main__':
  absltest.main()
