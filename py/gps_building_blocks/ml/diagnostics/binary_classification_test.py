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
"""Tests for gps_building_blocks.ml.diagnostics.binary_classification."""

from absl.testing import absltest
import numpy as np
import pandas as pd
import sklearn.metrics
from absl.testing import parameterized
from gps_building_blocks.ml.diagnostics import binary_classification

TEST_DATA = pd.DataFrame({
    'label': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
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


class BinaryClassificationDiagnosticsTest(parameterized.TestCase,
                                          absltest.TestCase):

  def test_calc_performance_metrics_returns_correct_values(self):
    expected_results = {
        'prop_positives': 0.5000,
        'auc_roc': 0.7100,
        'auc_pr': 0.7278,
        'binarize_threshold': 0.5000,
        'accuracy': 0.6500,
        'true_positive_rate': 0.7000,
        'true_negative_rate': 0.6000,
        'precision': 0.6364,
        'f1_score': 0.6667
    }

    rerults = (
        binary_classification.calc_performance_metrics(
            labels=np.array(TEST_DATA['label']),
            probability_predictions=np.array(TEST_DATA['prediction'])))

    self.assertDictEqual(expected_results, rerults)

  def test_resulted_bin_metrics_does_not_contain_nas(self):
    results = (
        binary_classification.calc_bin_metrics(
            labels=np.array(TEST_DATA['label']),
            probability_predictions=np.array(TEST_DATA['prediction']),
            number_bins=3))

    self.assertFalse(results.isna().values.any())

  def test_calc_bin_metrics_returns_correct_values(self):
    bin_number = [1, 2, 3]
    bin_size = [7, 5, 8]
    positive_instances = [5, 2, 3]
    precision = [0.7143, 0.4000, 0.3750]
    prop_positives = [0.5000, 0.5000, 0.5000]
    precision_uplift = [1.4286, 0.8000, 0.7500]
    coverage = [0.5000, 0.2000, 0.3000]

    results = (
        binary_classification.calc_bin_metrics(
            labels=np.array(TEST_DATA['label']),
            probability_predictions=np.array(TEST_DATA['prediction']),
            number_bins=3))

    self.assertListEqual(results['bin_number'].tolist(), bin_number)
    self.assertListEqual(results['bin_size'].tolist(), bin_size)
    self.assertListEqual(results['positive_instances'].tolist(),
                         positive_instances)
    self.assertListEqual(results['precision'].tolist(), precision)
    self.assertListEqual(results['prop_positives'].tolist(), prop_positives)
    self.assertListEqual(results['precision_uplift'].tolist(), precision_uplift)
    self.assertListEqual(results['coverage'].tolist(), coverage)

  def test_plot_bin_metrics_returns_bar_plots_with_correct_elements(self):
    bin_metrics = (
        binary_classification.calc_bin_metrics(
            labels=np.array(TEST_DATA['label']),
            probability_predictions=np.array(TEST_DATA['prediction']),
            number_bins=3))
    x_data = list(bin_metrics['bin_number'])
    y_data_precision = list(bin_metrics['precision'])
    y_data_precision_uplift = list(bin_metrics['precision_uplift'])
    y_data_coverage = list(bin_metrics['coverage'])

    plots = binary_classification.plot_bin_metrics(bin_metrics)
    plot_1 = plots[0]
    plot_2 = plots[1]
    plot_3 = plots[2]

    with self.subTest(name='test the elements of precision bar plot'):
      self.assertListEqual(
          x_data, [int(tick.get_text()) for tick in plot_1.get_xticklabels()])
      self.assertListEqual(y_data_precision,
                           [h.get_height() for h in plot_1.patches])
    with self.subTest(name='test the elements of precision uplift bar plot'):
      self.assertListEqual(
          x_data, [int(tick.get_text()) for tick in plot_2.get_xticklabels()])
      self.assertListEqual(y_data_precision_uplift,
                           [h.get_height() for h in plot_2.patches])
    with self.subTest(name='test the elements of coverage bar plot'):
      self.assertListEqual(
          x_data, [int(tick.get_text()) for tick in plot_3.get_xticklabels()])
      self.assertListEqual(y_data_coverage,
                           [h.get_height() for h in plot_3.patches])

  def test_calc_cumulative_bin_metrics_returns_correct_values(self):
    cumulative_bin_number = [1, 2, 3]
    bin_size = [7, 13, 20]
    bin_size_proportion = [0.3500, 0.6500, 1.0000]
    positive_instances = [5, 7, 10]
    precision = [0.7143, 0.5385, 0.5000]
    coverage = [0.5000, 0.7000, 1.0000]
    prop_label_positives = [0.5000, 0.5000, 0.5000]
    precision_uplift = [1.4286, 1.0770, 1.0000]

    results = (
        binary_classification.calc_cumulative_bin_metrics(
            labels=np.array(TEST_DATA['label']),
            probability_predictions=np.array(TEST_DATA['prediction']),
            number_bins=3))

    self.assertListEqual(results['cumulative_bin_number'].tolist(),
                         cumulative_bin_number)
    self.assertListEqual(results['bin_size'].tolist(), bin_size)
    self.assertListEqual(results['bin_size_proportion'].tolist(),
                         bin_size_proportion)
    self.assertListEqual(results['positive_instances'].tolist(),
                         positive_instances)
    self.assertListEqual(results['precision'].tolist(), precision)
    self.assertListEqual(results['coverage (recall)'].tolist(), coverage)
    self.assertListEqual(results['prop_label_positives'].tolist(),
                         prop_label_positives)
    self.assertListEqual(results['precision_uplift'].tolist(), precision_uplift)

  def test_plot_cumulative_bin_metrics_returns_correct_plots(self):
    cumulative_bin_metrics = (
        binary_classification.calc_cumulative_bin_metrics(
            labels=np.array(TEST_DATA['label']),
            probability_predictions=np.array(TEST_DATA['prediction']),
            number_bins=3))
    x_data = list(cumulative_bin_metrics['cumulative_bin_number'])
    y_data_precision = list(cumulative_bin_metrics['precision'])
    y_data_precision_uplift = list(cumulative_bin_metrics['precision_uplift'])
    y_data_coverage = list(cumulative_bin_metrics['coverage (recall)'])

    plots = (
        binary_classification.plot_cumulative_bin_metrics(
            cumulative_bin_metrics))
    plot_1 = plots[0]
    plot_2 = plots[1]
    plot_3 = plots[2]

    with self.subTest(name='test the elements of precision bar plot'):
      self.assertListEqual(
          x_data, [int(tick.get_text()) for tick in plot_1.get_xticklabels()])
      self.assertListEqual(y_data_precision,
                           [h.get_height() for h in plot_1.patches])
    with self.subTest(name='test the elements of precision uplift bar plot'):
      self.assertListEqual(
          x_data, [int(tick.get_text()) for tick in plot_2.get_xticklabels()])
      self.assertListEqual(y_data_precision_uplift,
                           [h.get_height() for h in plot_2.patches])
    with self.subTest(name='test the elements of coverage bar plot'):
      self.assertListEqual(
          x_data, [int(tick.get_text()) for tick in plot_3.get_xticklabels()])
      self.assertListEqual(y_data_coverage,
                           [h.get_height() for h in plot_3.patches])

  def test_plot_binned_features_return_plots_with_correct_elements(self):
    number_bins = 3
    prediction_column_name = 'prediction'

    # prepare results
    test_data = TEST_DATA.sort_values(
        by=prediction_column_name, ascending=False)
    test_data['bin_number'] = (
        number_bins -
        pd.qcut(test_data[prediction_column_name], q=number_bins, labels=False))
    # stats for the numerical feature
    num_binned_test_data = test_data[['bin_number', 'num_feature_1']]
    num_binned_test_data = num_binned_test_data.rename(
        columns={'num_feature_1': 'v'})
    num_bin_stats = (
        num_binned_test_data[['bin_number',
                              'v']].groupby('bin_number',
                                            as_index=False).agg('mean'))
    num_bin_stats.columns = ['bin_number', 'var_mean']

    # stats for the categorical feature
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
        binary_classification.plot_binned_features(
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
      self.assertListEqual(
          list(cat_bin_stats['proportion']),
          [round(h.get_height(), 5) for h in cat_plot.patches])

  def test_plot_predicted_probabilities(self):
    plot = binary_classification.plot_predicted_probabilities(
        labels=np.array(TEST_DATA['label']),
        probability_predictions=np.array(TEST_DATA['prediction']),
        colors=('b', 'g'),
        print_stats=True,
        fig_width=20,
        fig_height=15)

    with self.subTest(name='test the title of plot'):
      self.assertEqual('Distribution of predicted probabilities',
                       plot.get_title())
    with self.subTest(name='test the label of the plot'):
      preds_plot0 = TEST_DATA[TEST_DATA['label'] == 0]['prediction']
      preds_plot1 = TEST_DATA[TEST_DATA['label'] == 1]['prediction']
      expect_legends = [
          'class[%s]' % (str(0)) + ': mean=%.4f, std=%.4f, median=%.4f' %
          (np.mean(preds_plot0), np.std(preds_plot0), np.median(preds_plot0)),
          'class[%s]' % (str(1)) + ': mean=%.4f, std=%.4f, median=%.4f' %
          (np.mean(preds_plot1), np.std(preds_plot1), np.median(preds_plot1))
      ]
      actual_legends = [l.get_text() for l in plot.get_legend().get_texts()]
      self.assertListEqual(expect_legends, actual_legends)

  @parameterized.named_parameters(
      dict(
          testcase_name='test_plot_roc_curve',
          plot_name='roc',
          print_stats=True,
          fig_width=10,
          fig_height=10,
          curve_color='b'),
      dict(
          testcase_name='test_plot_pr_curve',
          plot_name='precision-recall',
          print_stats=True,
          fig_width=10,
          fig_height=10,
          curve_color='b'))
  def test_plots(self, plot_name, print_stats, fig_width, fig_height,
                 curve_color):
    if plot_name == 'roc':
      plot = binary_classification.plot_roc_curve(
          labels=np.array(TEST_DATA['label']),
          probability_predictions=np.array(TEST_DATA['prediction']),
          print_stats=print_stats,
          fig_width=fig_width,
          fig_height=fig_height,
          curve_color=curve_color)
      expected_title = 'AUC=0.7100'
    elif plot_name == 'precision-recall':
      plot = binary_classification.plot_precision_recall_curve(
          labels=np.array(TEST_DATA['label']),
          probability_predictions=np.array(TEST_DATA['prediction']),
          print_stats=print_stats,
          fig_width=fig_width,
          fig_height=fig_height,
          curve_color=curve_color)
      expected_title = 'Average Precision=%.4f' % sklearn.metrics.average_precision_score(
          np.array(TEST_DATA['label']), np.array(TEST_DATA['prediction']))
    else:
      raise NotImplementedError('plot name %s is not included in the tests.')

    with self.subTest(name='test the title of plot'):
      self.assertEqual(expected_title, plot.get_title())

    with self.subTest(name='test the lines of the plot'):
      lines = plot.get_lines()
      self.assertLen(lines, 1)
      self.assertEqual('b', lines[0].get_color())


if __name__ == '__main__':
  absltest.main()
