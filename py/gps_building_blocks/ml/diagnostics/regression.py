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
from typing import List, Optional, Sequence
from matplotlib import axes
from matplotlib import pyplot
import numpy as np
import pandas as pd
import scipy as sp
import seaborn as sns
from sklearn import metrics
from gps_building_blocks.ml import utils
from gps_building_blocks.ml.diagnostics import feature_insights


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


def calc_reg_bin_metrics(labels: np.ndarray,
                         predictions: np.ndarray,
                         number_bins: Optional[int] = 10,
                         decimal_points: Optional[int] = 4) -> pd.DataFrame:
  """Calculates performance metrics for each bin of the predictions.

  Args:
    labels: An array of true labels containing numeric values.
    predictions: An array of predictions containing numeric values.
    number_bins: Number of bins that we want to divide the ranked predictions
      into. Default is deciles (10 bins) such that the 1st bin contains the
      highest 10% of the predictions, the 2nd bin contains the next 10% of the
      predictions and so on.
    decimal_points: Number of decimal points to use when outputting the
      calculated performance metrics.

  Returns:
    bin_metrics: Following metrics calculated for each bin:
      mean_label: Mean of actual values in the bin.
      mean_prediction: Mean of predictions in the bin.
      mse: Mean squared error.
      rmse: Root mean squared error.
      msle: Mean squared log error
      mape: Mean absolute percentage error.
      r_squared: R-squared (Coefficient of Determination).
      corr: pearson_correlation coefficient.
  """
  utils.assert_label_and_prediction_length_match(labels, predictions)

  # Separate the predictions into bins.
  bins = pd.qcut(predictions, q=number_bins, labels=False, duplicates='drop')
  binned_data = pd.DataFrame(
      list(zip(labels, predictions, bins)),
      columns=['label', 'prediction', 'bin_number'])

  bin_metrics = binned_data.groupby(
      'bin_number', as_index=False).agg({
          'label': 'mean',
          'prediction': 'mean'
      }).rename(columns={
          'label': 'mean_label',
          'prediction': 'mean_prediction'
      })
  bin_metrics['mean_label'] = round(bin_metrics['mean_label'], decimal_points)
  bin_metrics['mean_prediction'] = round(bin_metrics['mean_prediction'],
                                         decimal_points)
  bin_metrics['mse'] = 0
  bin_metrics['rmse'] = 0
  bin_metrics['msle'] = 0
  bin_metrics['mape'] = 0
  bin_metrics['r_squared'] = 0
  bin_metrics['corr'] = 0

  for i in range(number_bins):
    (bin_labels, bin_predictions) = (
        binned_data[binned_data.bin_number == i]['label'].values,
        binned_data[binned_data.bin_number == i]['prediction'].values)
    bin_perf_metrics = calc_performance_metrics(bin_labels, bin_predictions,
                                                decimal_points)
    bin_metrics.loc[i, 'mse'] = bin_perf_metrics.mean_squared_error
    bin_metrics.loc[i, 'rmse'] = bin_perf_metrics.root_mean_squared_error
    bin_metrics.loc[i, 'msle'] = bin_perf_metrics.mean_squared_log_error
    bin_metrics.loc[i, 'mape'] = bin_perf_metrics.mean_absolute_percentage_error
    bin_metrics.loc[i, 'r_squared'] = bin_perf_metrics.r_squared
    bin_metrics.loc[i, 'corr'] = bin_perf_metrics.pearson_correlation

  bin_metrics['bin_number'] = number_bins - bin_metrics['bin_number']
  bin_metrics = bin_metrics.sort_values(['bin_number'])

  return bin_metrics


def plot_reg_bin_metrics(bin_metrics: pd.DataFrame,
                         fig_width: Optional[int] = 20,
                         fig_height: Optional[int] = 25,
                         title_fontsize: Optional[int] = 12,
                         axis_label_fontsize: Optional[int] = 10,
                         bar_color: Optional[str] = 'coolwarm') -> axes.Axes:
  """Plots the mean and mape of the bins of the actual and predicted probabilities.

  Args:
    bin_metrics: Performance metrics calculated for the bins of the predicted by
      calc_reg_bin_metrics() function.
    fig_width: Width of the figure.
    fig_height: Height of the figure.
    title_fontsize: Title font size of the plots.
    axis_label_fontsize: Axis label font size of the plots.
    bar_color: Color of the bar plot.

  Returns:
    plots: Bar plots of the mean, mape, mse, rmse, msle, rsquared and
    correlation of bins of the actual values and predictions.
  """
  _, plots = pyplot.subplots(nrows=4, ncols=2, figsize=(fig_width, fig_height))

  bin_metrics_mean = bin_metrics[[
      'bin_number', 'mean_label', 'mean_prediction'
  ]].melt(id_vars='bin_number')
  plot_1 = sns.barplot(
      ax=plots[0, 0],
      x='bin_number',
      y='value',
      hue='variable',
      data=bin_metrics_mean,
      palette=bar_color)
  plot_1.set_title(
      'Mean of actual and prediction value in each bin',
      fontsize=title_fontsize)
  plot_1.set_xlabel('Bins of predicted values', fontsize=axis_label_fontsize)
  plot_1.set_ylabel('Mean value', fontsize=axis_label_fontsize)

  plot_2 = sns.barplot(
      ax=plots[0, 1],
      x='bin_number',
      y='mape',
      data=bin_metrics,
      palette=bar_color)
  plot_2.set_title('MAPE in each bin', fontsize=title_fontsize)
  plot_2.set_xlabel('Bins of predicted values', fontsize=axis_label_fontsize)
  plot_2.set_ylabel('MAPE', fontsize=axis_label_fontsize)

  plot_3 = sns.barplot(
      ax=plots[1, 0],
      x='bin_number',
      y='mse',
      data=bin_metrics,
      palette=bar_color)
  plot_3.set_title('MSE in each bin', fontsize=title_fontsize)
  plot_3.set_xlabel('Bins of predicted values', fontsize=axis_label_fontsize)
  plot_3.set_ylabel('MSE', fontsize=axis_label_fontsize)

  plot_4 = sns.barplot(
      ax=plots[1, 1],
      x='bin_number',
      y='rmse',
      data=bin_metrics,
      palette=bar_color)
  plot_4.set_title('RMSE in each bin', fontsize=title_fontsize)
  plot_4.set_xlabel('Bins of predicted values', fontsize=axis_label_fontsize)
  plot_4.set_ylabel('RMSE', fontsize=axis_label_fontsize)

  plot_5 = sns.barplot(
      ax=plots[2, 0],
      x='bin_number',
      y='msle',
      data=bin_metrics,
      palette=bar_color)
  plot_5.set_title('MSLE in each bin', fontsize=title_fontsize)
  plot_5.set_xlabel('Bins of predicted values', fontsize=axis_label_fontsize)
  plot_5.set_ylabel('MSLE', fontsize=axis_label_fontsize)

  plot_6 = sns.barplot(
      ax=plots[2, 1],
      x='bin_number',
      y='r_squared',
      data=bin_metrics,
      palette=bar_color)
  plot_6.set_title('R squared in each bin', fontsize=title_fontsize)
  plot_6.set_xlabel('Bins of predicted values', fontsize=axis_label_fontsize)
  plot_6.set_ylabel('R squared', fontsize=axis_label_fontsize)

  plot_7 = sns.barplot(
      ax=plots[3, 0],
      x='bin_number',
      y='corr',
      data=bin_metrics,
      palette=bar_color)
  plot_7.set_title('Correlation in each bin', fontsize=title_fontsize)
  plot_7.set_xlabel('Bins of predicted values', fontsize=axis_label_fontsize)
  plot_7.set_ylabel('Correlation', fontsize=axis_label_fontsize)
  plots[3, 1].axis('off')

  return plots


def plot_confusion_matrix_bin_heatmap(
    labels: np.ndarray,
    predictions: np.ndarray,
    number_bins: Optional[int] = 10,
    fig_width: Optional[int] = 12,
    fig_height: Optional[int] = 12,
    title_fontsize: Optional[int] = 12,
    axis_label_fontsize: Optional[int] = 10,
    heatmap_color: Optional[str] = 'YlGnBu') -> axes.Axes:
  """Plots the heatmap of the bins of the actual and predicted values.

  Args:
    labels: An array of true labels containing numeric values.
    predictions: An array of predictions containing numeric values.
    number_bins: Number of bins that we want to divide the ranked predictions
      into. Default is deciles (10 bins) such that the 1st bin contains the
      highest 10% of the predictions, the 2nd bin contains the next 10% of the
      predictions and so on.
    fig_width: Width of the figure.
    fig_height: Height of the figure.
    title_fontsize: Title font size of the plots.
    axis_label_fontsize: Axis label font size of the plots.
    heatmap_color: Color of the heatmap plot.

  Returns:
    plot: Heatmap of the bins of the actual and predicted values.
  """
  utils.assert_label_and_prediction_length_match(labels, predictions)

  bins = pd.qcut(predictions, q=number_bins, labels=False, duplicates='drop')
  bins_label = pd.qcut(labels, q=number_bins, labels=False, duplicates='drop')
  binned_data = pd.DataFrame(
      list(zip(labels, predictions, bins, bins_label)),
      columns=['labels', 'predictions', 'bin_number', 'bin_number_label'])

  conf_matrix = metrics.confusion_matrix(binned_data['bin_number_label'],
                                         binned_data['bin_number'])
  conf_matrix = conf_matrix / np.sum(conf_matrix)

  _, plot = pyplot.subplots(figsize=(fig_width, fig_height))
  plot = sns.heatmap(conf_matrix, cbar=False, cmap=heatmap_color, annot=True)
  plot.set_title(
      'Heatmap of the bins of the actual and predicted values',
      fontsize=title_fontsize)
  plot.set_xlabel('Actual value bins', fontsize=axis_label_fontsize)
  plot.set_ylabel('Prediction value bins', fontsize=axis_label_fontsize)

  return plot


def plot_binned_features(
    data: pd.DataFrame,
    prediction_column_name: str,
    feature_names: Sequence[str],
    feature_types: Sequence[str],
    number_bins: Optional[int] = 10,
    fig_width: Optional[int] = 10,
    fig_height: Optional[int] = 5,
    numerical_feature_color: Optional[str] = 'coral',
    title_fontsize: Optional[int] = 12,
    x_label_fontsize: Optional[int] = 12,
    y_label_fontsize: Optional[int] = 12,
    tick_label_fontsize: Optional[int] = 10,
    legend_font_size: Optional[int] = 10) -> List[axes.Axes]:
  """Plots the distributions of features for the bins of the predictions.

  Args:
    data: Dataset containing the features to be plotted with the regression
      predictions.
    prediction_column_name: Column name of the predictions.
    feature_names: Columns names of the features to be plotted.
    feature_types: Types of the corresponding features to be plotted in order of
      the values in feature_names. Should only contain 'numerical' and
      'categorical' as values.
    number_bins: Number of bins that we want to divide the ranked predictions
      into. Default is deciles (10 bins) such that the 1st bin contains the
      highest 10% of the predictions, the 2nd bin contains the next 10% of the
      predictions and so on.
    fig_width: Width of the figure.
    fig_height: Height of the figure.
    numerical_feature_color: Color of the numerical feature bar plot.
    title_fontsize: Font size of the figure title.
    x_label_fontsize: Font size of the x axis labels.
    y_label_fontsize: Font size of the y axis labels.
    tick_label_fontsize: Font size of the x and y axis tick labels.
    legend_font_size: Font size of the legend.

  Returns:
    plots: Plots of the selected features.
  """
  return feature_insights.plot_binned_features(
      data=data,
      prediction_column_name=prediction_column_name,
      feature_names=feature_names,
      feature_types=feature_types,
      number_bins=number_bins,
      fig_width=fig_width,
      fig_height=fig_height,
      numerical_feature_color=numerical_feature_color,
      title_fontsize=title_fontsize,
      x_label_fontsize=x_label_fontsize,
      y_label_fontsize=y_label_fontsize,
      tick_label_fontsize=tick_label_fontsize,
      legend_font_size=legend_font_size)
