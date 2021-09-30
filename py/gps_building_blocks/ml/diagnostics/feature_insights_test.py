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
"""Tests for gps_building_blocks.ml.diagnostics.feature_insights."""

from absl.testing import absltest
import pandas as pd
from gps_building_blocks.ml.diagnostics import feature_insights

TEST_DATA = pd.DataFrame({
    'prediction': [
        0.7, 0.63, 0.4, 0.77, 0.45, 0.8, 0.41, 0.82, 0.7, 0.6, 0.5, 0.45, 0.74,
        0.11, 0.21, 0.05, 0.67, 0.79, 0.60, 0.10
    ],
    'num_feature_1': [
        20, 30, 22.5, 19, 30, 32, 15.6, 17.87, 25.45, 17.3, 30.2, 33, 27.5,
        25.1, 35.6, 33.26, 38.5, 31.23, 28.44, 30.32
    ],
    'cat_feature_1': [
        'M', 'M', 'F', 'M', 'M', 'F', 'M', 'M', 'F', 'F', 'F', 'F', 'M', 'M',
        'F', 'M', 'F', 'M', 'M', 'F'
    ]
})


class FeatureInsightsTest(absltest.TestCase):

  def test_plot_binned_features_return_plots_with_correct_elements(self):
    number_bins = 3
    prediction_column_name = 'prediction'

    # Prepare results.
    test_data = TEST_DATA.sort_values(
        by=prediction_column_name, ascending=False)
    test_data['bin_number'] = (
        number_bins -
        pd.qcut(test_data[prediction_column_name], q=number_bins, labels=False))

    # Stats for the numerical feature
    num_binned_test_data = test_data[['bin_number', 'num_feature_1']]
    num_binned_test_data = num_binned_test_data.rename(
        columns={'num_feature_1': 'v'})
    num_bin_stats = (
        num_binned_test_data[['bin_number',
                              'v']].groupby('bin_number',
                                            as_index=False).agg('mean'))
    num_bin_stats.columns = ['bin_number', 'var_mean']

    # Stats for the categorical feature
    cat_binned_test_data = test_data[['bin_number', 'cat_feature_1']]
    cat_binned_test_data = cat_binned_test_data.rename(
        columns={'cat_feature_1': 'categories'})
    bin_counts = (
        cat_binned_test_data.groupby('bin_number', as_index=False).count())
    bin_counts.columns = ['bin_number', 'total_count']
    cat_binned_test_data['temp_column'] = 1
    bin_category_counts = (
        cat_binned_test_data.groupby(['bin_number', 'categories'],
                                     as_index=False).count())
    bin_category_counts.columns = ['bin_number', 'categories', 'count']
    cat_bin_stats = pd.merge(bin_category_counts, bin_counts, on='bin_number')
    cat_bin_stats['proportion'] = (
        round((cat_bin_stats['count'] / cat_bin_stats['total_count']) * 100, 5))
    cat_bin_stats = cat_bin_stats.sort_values('categories')

    num_plot, cat_plot = (
        feature_insights.plot_binned_features(
            data=TEST_DATA,
            number_bins=number_bins,
            prediction_column_name=prediction_column_name,
            feature_names=('num_feature_1', 'cat_feature_1'),
            feature_types=('numerical', 'categorical')))

    with self.subTest(name='test the elements of numerical feature plot'):
      self.assertEqual('num_feature_1', num_plot.get_title())
      self.assertListEqual(
          list(num_bin_stats['bin_number']),
          [int(tick.get_text()) for tick in num_plot.get_xticklabels()])
      self.assertListEqual(
          list(num_bin_stats['var_mean']),
          [h.get_height() for h in num_plot.patches])

    with self.subTest(name='test the elements of categorical feature plot'):
      self.assertEqual('cat_feature_1', cat_plot.get_title())
      self.assertListEqual(
          list(set(cat_bin_stats['bin_number'])),
          [int(tick.get_text()) for tick in cat_plot.get_xticklabels()])
      self.assertSequenceAlmostEqual(
          list(cat_bin_stats['proportion']),
          [h.get_height() for h in cat_plot.patches], 5)

if __name__ == '__main__':
  absltest.main()
