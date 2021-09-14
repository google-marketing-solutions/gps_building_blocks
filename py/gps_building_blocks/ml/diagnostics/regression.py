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
"""Produces plots and statistics to diagnose a regression model.

Specially useful when diagnosing a Lifetime Value (LTV) model.
"""

import dataclasses
from typing import Optional

from matplotlib import axes
from matplotlib import pyplot
import numpy as np
import scipy as sp
from sklearn import metrics

from gps_building_blocks.ml import utils


@dataclasses.dataclass
class _PerformanceMetrics:
  """A container class for the following regression diagnostics metrics.

  Mean squared error.
  Root mean squared error.
  Mean squared log error.
  Mean absolute error.
  Mean absolute percentage error.
  R-squared (Coefficient of Determination).
  Pearson correlation between actual and predicted labels.
  """
  mean_squared_error: float
  root_mean_squared_error: float
  mean_squared_log_error: float
  mean_absolute_error: float
  mean_absolute_percentage_error: float
  r_squared: float
  pearson_correlation: float


def calc_performance_metrics(
    labels: np.ndarray,
    predictions: np.ndarray,
    decimal_points: Optional[int] = 4) -> _PerformanceMetrics:
  """Calculates performance metrics related to a regression model.

  Args:
    labels: An array of true labels containing numeric values.
    predictions: An array of predictions containing numeric values.
    decimal_points: Number of decimal points to use when outputting the
      calculated performance metrics.

  Returns:
    Object of _PerformanceMetrics class containing the regression diagnostics
      metrics.
  """
  utils.assert_label_and_prediction_length_match(labels,
                                                 predictions)

  mse = metrics.mean_squared_error(labels, predictions)
  rmse = np.sqrt(mse)
  msle = np.sqrt(metrics.mean_squared_log_error(labels, predictions))
  mae = metrics.mean_absolute_error(labels, predictions)
  mape = metrics.mean_absolute_percentage_error(labels, predictions)
  r2 = metrics.r2_score(labels, predictions)
  corr = sp.stats.pearsonr(labels, predictions)[0]

  return _PerformanceMetrics(
      mean_squared_error=round(mse, decimal_points),
      root_mean_squared_error=round(rmse, decimal_points),
      mean_squared_log_error=round(msle, decimal_points),
      mean_absolute_error=round(mae, decimal_points),
      mean_absolute_percentage_error=round(mape, decimal_points),
      r_squared=round(r2, decimal_points),
      pearson_correlation=round(corr, decimal_points))


def plot_prediction_residuals(labels: np.ndarray,
                              predictions: np.ndarray,
                              fig_width: Optional[int] = 12,
                              fig_height: Optional[int] = 12,
                              title_fontsize: Optional[int] = 12,
                              axis_label_fontsize: Optional[int] = 10,
                              use_log: Optional[bool] = False) -> axes.Axes:
  """Plots scatter plots of true labels and residuals versus the predicted values.

  Args:
    labels: An array of true labels containing numeric values.
    predictions: An array of predictions containing numeric values.
    fig_width: Width of the figure.
    fig_height: Height of the figure.
    title_fontsize: Title font size of the plots.
    axis_label_fontsize: Axis label font size of the plots.
    use_log: Boolean value indicating taking logarithm of the actual and
      predicted values.

  Returns:
    plots: Scatter plots of true values and residuals versus the predicted
    values.
  """
  utils.assert_label_and_prediction_length_match(labels, predictions)

  _, plots = pyplot.subplots(nrows=2, figsize=(fig_width, fig_height))
  if use_log:
    plots[0].scatter(x=np.log1p(predictions), y=np.log1p(labels))
    plots[0].set_title(
        'Scatter plot of true label values versus predicted values with log transformation',
        fontsize=title_fontsize)
    plots[0].set_xlabel(
        'Logarithm of predicted values', fontsize=axis_label_fontsize)
    plots[0].set_ylabel(
        'Logarithm of label values', fontsize=axis_label_fontsize)
  else:
    plots[0].scatter(x=predictions, y=labels)
    plots[0].set_title(
        'Scatter plot of true label values versus predicted values',
        fontsize=title_fontsize)
    plots[0].set_xlabel('Predicted values', fontsize=axis_label_fontsize)
    plots[0].set_ylabel('Label values', fontsize=axis_label_fontsize)

  plots[1].scatter(x=predictions, y=labels - predictions)
  plots[1].set_title(
      'Scatter plot of residuals versus predicted values',
      fontsize=title_fontsize)
  plots[1].set_xlabel('Predicted values', fontsize=axis_label_fontsize)
  plots[1].set_ylabel('Residuals', fontsize=axis_label_fontsize)
  plots[1].axhline(0, linestyle='--')

  return plots
