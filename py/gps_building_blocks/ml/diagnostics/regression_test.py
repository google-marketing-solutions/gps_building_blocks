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
import numpy as np
import pandas as pd
from absl.testing import parameterized
from gps_building_blocks.ml.diagnostics import regression

_TEST_DATA = pd.DataFrame({
    'label': [
        60.54, 51.74, 31.52, 12.56, 37.98, 74.24, 55.36, 58.16, 39.93, 26.3,
        89.17, 31.98, 10.61, 40.15, 60.62, 54.33, 68.9, 34.09, 25.33, 79.46
    ],
    'prediction': [
        65.55, 58.08, 32.97, 21.39, 42.53, 79.83, 63.19, 68.28, 48.45, 29.57,
        95.5, 36.91, 17.45, 46.88, 62.13, 55.53, 75.3, 43.0, 33.19, 82.34
    ]
})


class RegressionDiagnosticsTest(parameterized.TestCase, absltest.TestCase):

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

    self.assertAlmostEqual(mean_squared_error, results.mean_squared_error)
    self.assertAlmostEqual(root_mean_squared_error,
                           results.root_mean_squared_error)
    self.assertAlmostEqual(mean_squared_log_error,
                           results.mean_squared_log_error)
    self.assertAlmostEqual(mean_absolute_error, results.mean_absolute_error)
    self.assertAlmostEqual(mean_absolute_percentage_error,
                           results.mean_absolute_percentage_error)
    self.assertAlmostEqual(r_squared, results.r_squared)
    self.assertAlmostEqual(pearson_correlation, results.pearson_correlation)

  @parameterized.named_parameters(
      dict(testcase_name='test_plot_using_log', use_log_parameter=True),
      dict(testcase_name='test_plot_without_log', use_log_parameter=False))
  def test_plot_prediction_residuals(self, use_log_parameter):
    predictions = np.array(_TEST_DATA['prediction'])
    labels = np.array(_TEST_DATA['label'])

    plots = regression.plot_prediction_residuals(
        labels=labels, predictions=predictions, use_log=use_log_parameter)
    plot_1, plot_2 = plots
    if use_log_parameter:
      x_data_expected = list(np.log1p(predictions))
      y_data_expected = list(np.log1p(labels))
      expected_title = ('Scatter plot of true label values versus predicted '
                        'values with log transformation')
    else:
      x_data_expected = list(predictions)
      y_data_expected = list(labels)
      expected_title = ('Scatter plot of true label values versus predicted '
                        'values')

    x_data_residual_expected = list(predictions)
    y_data_residual_expected = list(labels - predictions)
    expected_title_residual = ('Scatter plot of residuals versus predicted '
                               'values')

    with self.subTest(name='test the title of scatter plot'):
      self.assertEqual(expected_title, plot_1.get_title())
    with self.subTest(name='test the title of scatter plot of residuals'):
      self.assertEqual(expected_title_residual, plot_2.get_title())
    with self.subTest(name='test the elements of true label value plot'):
      self.assertListEqual(
          x_data_expected,
          [xydata[0] for xydata in plot_1.collections[0].get_offsets()])
      self.assertListEqual(
          y_data_expected,
          [xydata[1] for xydata in plot_1.collections[0].get_offsets()])
    with self.subTest(name='test the elements of residuals plot'):
      self.assertListEqual(
          x_data_residual_expected,
          [xydata[0] for xydata in plot_2.collections[0].get_offsets()])
      self.assertListEqual(
          y_data_residual_expected,
          [xydata[1] for xydata in plot_2.collections[0].get_offsets()])

  def test_calc_reg_bin_metrics_returns_correct_values(self):
    bin_number = [1, 2, 3]
    mean_label = [69.4043, 46.8100, 25.1829]
    mean_prediction = [75.7129, 52.3450, 30.5729]
    mse = [44.1993, 40.1978, 34.9053]
    rmse = [6.6483, 6.3402, 5.9081]
    msle = [0.0976, 0.1437, 0.2880]
    mape = [0.0962, 0.1353, 0.3003]
    r_squared = [0.6629, 0.5406, 0.6079]
    corr = [0.9856, 0.9791, 0.9741]

    results = (
        regression.calc_reg_bin_metrics(
            labels=np.array(_TEST_DATA['label']),
            predictions=np.array(_TEST_DATA['prediction']),
            number_bins=3))

    self.assertListEqual(results['bin_number'].tolist(), bin_number)
    self.assertListEqual(results['mean_label'].tolist(), mean_label)
    self.assertListEqual(results['mean_prediction'].tolist(), mean_prediction)
    self.assertListEqual(results['mse'].tolist(), mse)
    self.assertListEqual(results['rmse'].tolist(), rmse)
    self.assertListEqual(results['msle'].tolist(), msle)
    self.assertListEqual(results['mape'].tolist(), mape)
    self.assertListEqual(results['r_squared'].tolist(), r_squared)
    self.assertListEqual(results['corr'].tolist(), corr)

  def test_plot_reg_bin_metrics_returns_bar_plots_with_correct_elements(self):
    bin_metrics = regression.calc_reg_bin_metrics(
        labels=np.array(_TEST_DATA['label']),
        predictions=np.array(_TEST_DATA['prediction']),
        number_bins=3)
    plots = regression.plot_reg_bin_metrics(bin_metrics)
    x_data = list(bin_metrics['bin_number'])
    y_data_mean = list(bin_metrics['mean_label']) + list(
        bin_metrics['mean_prediction'])
    y_data_mse = list(bin_metrics['mse'])
    y_data_rmse = list(bin_metrics['rmse'])
    y_data_msle = list(bin_metrics['msle'])
    y_data_mape = list(bin_metrics['mape'])
    y_data_rsquared = list(bin_metrics['r_squared'])
    y_data_corr = list(bin_metrics['corr'])

    with self.subTest(name='test the elements of each metirc bar plot'):
      for plot, x_data_expected, y_data_expected in [
          (plots[0, 0], x_data, y_data_mean), (plots[0,
                                                     1], x_data, y_data_mape),
          (plots[1, 0], x_data, y_data_mse), (plots[1, 1], x_data, y_data_rmse),
          (plots[2, 0], x_data, y_data_msle),
          (plots[2, 1], x_data, y_data_rsquared),
          (plots[3, 0], x_data, y_data_corr)
      ]:
        self.assertListEqual(
            x_data_expected,
            [int(tick.get_text()) for tick in plot.get_xticklabels()])
        self.assertListEqual(y_data_expected,
                             [h.get_height() for h in plot.patches])

  def test_heatmap_returns_correct_values(self):
    predictions = np.array(_TEST_DATA['prediction'])
    labels = np.array(_TEST_DATA['label'])

    plot = regression.plot_confusion_matrix_bin_heatmap(
        labels, predictions, number_bins=3)
    bin_number = [0, 1, 2]
    with self.subTest(name='test the title of heatmap'):
      self.assertEqual('Heatmap of the bins of the actual and predicted values',
                       plot.get_title())
    with self.subTest(name='test the axis elements of heatmap'):
      self.assertListEqual(
          bin_number, [int(tick.get_text()) for tick in plot.get_xticklabels()])
      self.assertListEqual(
          bin_number, [int(tick.get_text()) for tick in plot.get_yticklabels()])


if __name__ == '__main__':
  absltest.main()
