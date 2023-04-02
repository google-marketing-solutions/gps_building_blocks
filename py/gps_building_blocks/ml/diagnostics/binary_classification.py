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
"""Produces plots and statistics to diagnose a binary classification model.

Specially useful when diagnosing a propensity model.
"""

from typing import Dict, List, Optional, Sequence
from matplotlib import axes
from matplotlib import pyplot
import numpy as np
import pandas as pd
import seaborn as sns
import sklearn.metrics
from gps_building_blocks.ml import utils
from gps_building_blocks.ml.diagnostics import feature_insights


def calc_performance_metrics(
    labels: np.ndarray,
    probability_predictions: np.ndarray,
    binarize_threshold: Optional[float] = None,
    decimal_points: Optional[int] = 4,
) -> Dict[str, float]:
  """Calculates performance metrics related to a binary classification model.

  Args:
    labels: An array of true binary labels represented by 1.0 and 0.0.
    probability_predictions: An array of predicted probabilities between 0.0 and
      1.0.
    binarize_threshold: Probability threshold to be used to binarize the
      predicted probabilities. By default the proportion of positive instances
      in the labels is used.
    decimal_points: Number of decimal points to use when outputting the
      calculated performance metrics.

  Returns:
    metrics: Dictionary of the following performance metric
      {prop_positives: Proportion of instances where label = 1.0,
       auc_roc: Area under the recall vs (1-specificity) (ROC) curve,
       auc_pr: Area under the recall vs precision (ROC) curve.
       Following metrics are calculated after binarizing the predicted
       probabilities based on the given binarize_threshold,
       accuracy: Total accuracy of the predictions,
       true_positive_rate (recall or sensitivity): True positive rate,
       true_negative_rate (specificity): True negative rate,
       precision: Precision (confidence) of the true positive predictions,
       f1_score: F1 score of sensitivity and specificity,
       precision_uplift: Uplift of the precision compared to random prediction}
  """
  utils.assert_label_values_are_valid(labels)
  utils.assert_prediction_values_are_valid(probability_predictions)
  utils.assert_label_and_prediction_length_match(labels,
                                                 probability_predictions)

  num_positives = labels.sum()
  prop_positives = float(num_positives) / len(labels)

  if binarize_threshold is None:
    binarize_threshold = prop_positives

  # Calculate auc metrics.
  auc_roc = round(
      sklearn.metrics.roc_auc_score(labels, probability_predictions),
      decimal_points)
  auc_pr = round(
      sklearn.metrics.average_precision_score(labels, probability_predictions),
      decimal_points)

  # Binarize the predictions.
  binarized_predictions = ((probability_predictions >
                            binarize_threshold).astype(int))

  # Calculate metrics based on binarized predictions.
  accuracy = sklearn.metrics.accuracy_score(labels, binarized_predictions)
  tp_rate = sklearn.metrics.recall_score(
      labels, binarized_predictions, pos_label=1)
  tn_rate = sklearn.metrics.recall_score(
      labels, binarized_predictions, pos_label=0)
  precision = sklearn.metrics.precision_score(labels, binarized_predictions)
  f1_score = sklearn.metrics.f1_score(labels, binarized_predictions)

  return {
      'prop_positives': round(prop_positives, decimal_points),
      'auc_roc': round(auc_roc, decimal_points),
      'auc_pr': round(auc_pr, decimal_points),
      'binarize_threshold': round(binarize_threshold, decimal_points),
      'accuracy': round(accuracy, decimal_points),
      'true_positive_rate': round(tp_rate, decimal_points),
      'true_negative_rate': round(tn_rate, decimal_points),
      'precision': round(precision, decimal_points),
      'f1_score': round(f1_score, decimal_points)
  }


def calc_bin_metrics(labels: np.ndarray,
                     probability_predictions: np.ndarray,
                     number_bins: Optional[int] = 10,
                     decimal_points: Optional[int] = 4) -> pd.DataFrame:
  """Calculates performance metrics for each bin of the predictions.

  Args:
    labels: An array of true binary labels represented by 1.0 and 0.0.
    probability_predictions: An array of predicted probabilities between 0.0 and
      1.0.
    number_bins: Number of bins that we want to divide the ranked predictions
      into. Default is deciles (10 bins) such that the 1st bin contains the
      highest 10% of the predictions, the 2nd bin contains the next 10% of the
      predictions and so on.
      decimal_points: Number of decimal points to use when outputting the
        calculated performance metrics.

  Returns:
    bin_metrics: Following metrics calculated for each bin.
      bin_number: Bin number starting from 1.
      bin_size: Total numbers of instances in the bin,
      positive_instances: Numbers of positive instances in the bin,
      precision: Proportion of positive instances out of all the instances
        in the bin,
      coverage: Proportion of positives instances out of all the positive
        instances in the dataset
      prop_positives: Proportion of positive instances in the label,
      precision_uplift: Uplift of precision compared to the precision
        of the random prediction (prop_positives).
  """
  utils.assert_label_values_are_valid(labels)
  utils.assert_prediction_values_are_valid(probability_predictions)
  utils.assert_label_and_prediction_length_match(labels,
                                                 probability_predictions)

  # Separate the probability_predictions into bins of equal size.
  binned_data = pd.DataFrame(
      list(zip(labels, probability_predictions)),
      columns=['label', 'prediction'])
  binned_data = binned_data.sort_values('prediction').reset_index()
  # To avoid duplicate edges of bins use the index in the qcat function below.
  binned_data['bin_number'] = pd.qcut(binned_data.index,
                                      q=number_bins, labels=False)

  # Calculate the metrics for each bin.
  total_instances = (
      binned_data[['bin_number', 'label']].groupby('bin_number').count())
  total_instances.columns = ['bin_size']
  total_instances = total_instances.reset_index()
  positive_instances = (
      binned_data.loc[binned_data['label'] > 0][[
          'bin_number', 'label'
      ]].groupby('bin_number').count())
  positive_instances.columns = ['positive_instances']
  positive_instances = positive_instances.reset_index()

  bin_metrics = pd.merge(
      total_instances, positive_instances, on='bin_number', how='left')
  bin_metrics.fillna(0, inplace=True)
  bin_metrics['precision'] = (
      bin_metrics['positive_instances'] / bin_metrics['bin_size'])
  bin_metrics['precision'] = [
      round(val, decimal_points) for val in bin_metrics['precision']
  ]
  prop_positives = round(labels[labels == 1.0].shape[0] / len(labels),
                         decimal_points)
  bin_metrics['prop_positives'] = prop_positives
  # Convert bin_number from zero-based offset to 1-based offset.
  bin_metrics['bin_number'] = bin_metrics['bin_number'] + 1
  bin_metrics['precision_uplift'] = bin_metrics['precision'] / prop_positives
  bin_metrics['precision_uplift'] = [
      round(val, decimal_points) for val in bin_metrics['precision_uplift']
  ]
  bin_metrics['coverage'] = (
      bin_metrics['positive_instances'] /
      sum(bin_metrics['positive_instances']))
  bin_metrics['coverage'] = [
      round(val, decimal_points) for val in bin_metrics['coverage']
  ]

  # Reverse the order of bin numbers such that bin 1 has the highest
  # predicted probability.
  bin_metrics['bin_number'] = number_bins - bin_metrics['bin_number'] + 1
  bin_metrics = bin_metrics.sort_values(['bin_number']).reset_index(drop=True)

  return bin_metrics


def plot_bin_metrics(bin_metrics: pd.DataFrame,
                     fig_width: Optional[int] = 20,
                     fig_height: Optional[int] = 15,
                     title_fontsize: Optional[int] = 12,
                     axis_label_fontsize: Optional[int] = 10,
                     axis_tick_fontsize: Optional[int] = 8,
                     precision_bar_color: Optional[str] = 'blue',
                     precision_uplift_bar_color: Optional[str] = 'blue',
                     coverage_bar_color: Optional[str] = 'red') -> axes.Axes:
  """Plots the performance metrics of the bins of the predicted probabilities.

  Args:
    bin_metrics: Performance metrics calculated for the bins of the predicted
      probabilities by calc_bin_metrics() function.
    fig_width: Width of the figure.
    fig_height: Height of the figure.
    title_fontsize: Title font size of the plots.
    axis_label_fontsize: Axis label font size of the plots.
    axis_tick_fontsize: Axis tick font size of the plots.
    precision_bar_color: Color of the precision bar plot.
    precision_uplift_bar_color: Color of the precision uplift bar plot.
    coverage_bar_color: Color of the coverage bar plot.

  Returns:
    plots: Bar plots of the precision, precision_uplift and coverage of the
      bins of the predicted probabilities.
  """
  _, plots = pyplot.subplots(nrows=3, figsize=(fig_width, fig_height))

  plot_1 = bin_metrics.plot.bar(
      x='bin_number', y='precision', ax=plots[0], color=precision_bar_color)
  plot_1.set_title('Precision of the bins ', fontsize=title_fontsize)
  plot_1.set_xlabel('Bin', fontsize=axis_label_fontsize)
  plot_1.set_ylabel('Precision', fontsize=axis_label_fontsize)
  plot_1.tick_params(labelsize=axis_tick_fontsize)

  plot_2 = bin_metrics.plot.bar(
      x='bin_number',
      y='precision_uplift',
      ax=plots[1],
      color=precision_uplift_bar_color)
  plot_2.set_title(
      'Precision uplift of the bins (compared to random prediction)',
      fontsize=title_fontsize)
  plot_2.set_xlabel('Bin', fontsize=axis_label_fontsize)
  plot_2.set_ylabel('Precision Uplift', fontsize=axis_label_fontsize)
  plot_2.tick_params(labelsize=axis_tick_fontsize)

  plot_3 = bin_metrics.plot.bar(
      x='bin_number', y='coverage', ax=plots[2], color=coverage_bar_color)
  plot_3.set_title(
      ('Proportion of positives instances of the bins (out of all the'
       'positives)'),
      fontsize=title_fontsize)
  plot_3.set_xlabel('Bin', fontsize=axis_label_fontsize)
  plot_3.set_ylabel('Coverage', fontsize=axis_label_fontsize)
  plot_3.tick_params(labelsize=axis_tick_fontsize)

  return plots


def calc_cumulative_bin_metrics(
    labels: np.ndarray,
    probability_predictions: np.ndarray,
    number_bins: int = 10,
    decimal_points: Optional[int] = 4) -> pd.DataFrame:
  """Calculates performance metrics for cumulative bins of the predictions.

  Args:
    labels: An array of true binary labels represented by 1.0 and 0.0.
    probability_predictions: An array of predicted probabilities between 0.0 and
      1.0.
    number_bins: Number of cumulative bins that we want to divide the ranked
      predictions into. Default is 10 bins such that the 1st bin contains the
      highest 10% of the predictions, 2nd bin contains the highest 20% of the
      predictions and so on.
      decimal_points: Number of decimal points to use when outputting the
        calculated performance metrics.

  Returns:
    bin_metrics: Following metrics calculated for each cumulative bin.
      cumulative_bin_number: Bin number starting from 1.
      bin_size: Total numbers of instances in the bin,
      bin_size_proportion: Proportion of instances in the bin out of all the
        instances in the labels.
      positive_instances: Numbers of positive instances in the bin,
      precision: Proportion of positive instances out of all the instances
        in the bin,
      coverage (recall): Proportion of positives instances in the bin out of
        all the positive instances in the labels,
      prop_label_positives: Proportion of positive instances in the labels,
      precision_uplift: Uplift of precision of the bin compared to the
        precision of the random prediction (prop_label_positives).
  """
  utils.assert_label_values_are_valid(labels)
  utils.assert_prediction_values_are_valid(probability_predictions)
  utils.assert_label_and_prediction_length_match(labels,
                                                 probability_predictions)

  # Separate the probability_predictions into bins.
  label_predictions = pd.DataFrame(
      list(zip(labels, probability_predictions)),
      columns=['label', 'prediction'])
  label_predictions = label_predictions.sort_values(
      by='prediction', ascending=False)
  number_total_instances = label_predictions.shape[0]
  equal_bin_size = number_total_instances / number_bins
  number_total_positive_instances = label_predictions[
      label_predictions['label'] > 0].shape[0]
  prop_label_positives = round(
      number_total_positive_instances / number_total_instances, decimal_points)

  cumulative_bin_metrics_list = list()

  for i in range(1, (number_bins + 1)):
    current_bin_size = round(equal_bin_size * i)
    bin_size_proportion = round(current_bin_size / number_total_instances,
                                decimal_points)
    bin_instances = label_predictions.head(current_bin_size)
    number_bin_positive_instances = bin_instances[
        bin_instances['label'] > 0].shape[0]
    bin_precision = round(number_bin_positive_instances / current_bin_size,
                          decimal_points)
    bin_recall = round(
        number_bin_positive_instances / number_total_positive_instances,
        decimal_points)
    bin_precision_uplift = round(bin_precision / prop_label_positives,
                                 decimal_points)

    cumulative_bin_metrics_list.append(
        (i, current_bin_size, bin_size_proportion,
         number_bin_positive_instances, bin_precision, bin_recall,
         prop_label_positives, bin_precision_uplift))

  return pd.DataFrame(
      cumulative_bin_metrics_list,
      columns=[
          'cumulative_bin_number', 'bin_size', 'bin_size_proportion',
          'positive_instances', 'precision', 'coverage (recall)',
          'prop_label_positives', 'precision_uplift'
      ])


def plot_cumulative_bin_metrics(
    cumulative_bin_metrics: pd.DataFrame,
    fig_width: Optional[int] = 20,
    fig_height: Optional[int] = 15,
    title_fontsize: Optional[int] = 12,
    axis_label_fontsize: Optional[int] = 10,
    axis_tick_fontsize: Optional[int] = 8,
    precision_bar_color: Optional[str] = 'blue',
    precision_uplift_bar_color: Optional[str] = 'blue',
    coverage_bar_color: Optional[str] = 'red') -> axes.Axes:
  """Plots the performance metrics of the cumulative bins of the predictions.

  Args:
    cumulative_bin_metrics: Performance metrics calculated for the cumulative
      bins of the predicted probabilities by calc_cumulative_bin_metrics()
      function.
    fig_width: Width of the figure.
    fig_height: Height of the figure.
    title_fontsize: Title font size of the plots.
    axis_label_fontsize: Axis label font size of the plots.
    axis_tick_fontsize: Axis tick font size of the plots.
    precision_bar_color: Color of the precision bar plot.
    precision_uplift_bar_color: Color of the precision uplift bar plot.
    coverage_bar_color: Color of the coverage (recall) bar plot.

  Returns:
    plots: Bar plots of the precision, precision_uplift and coverage (recall)
      of the bins of the predicted probabilities.
  """
  _, plots = pyplot.subplots(nrows=3, figsize=(fig_width, fig_height))

  plot_1 = cumulative_bin_metrics.plot.bar(
      x='cumulative_bin_number',
      y='precision',
      ax=plots[0],
      color=precision_bar_color)
  plot_1.set_title('Precision of the cumulative bins ', fontsize=title_fontsize)
  plot_1.set_xlabel('Cumulative Bin', fontsize=axis_label_fontsize)
  plot_1.set_ylabel('Precision', fontsize=axis_label_fontsize)
  plot_1.tick_params(labelsize=axis_tick_fontsize)

  plot_2 = cumulative_bin_metrics.plot.bar(
      x='cumulative_bin_number',
      y='precision_uplift',
      ax=plots[1],
      color=precision_uplift_bar_color)
  plot_2.set_title(('Precision uplift of the cumulative bins (compared to '
                    'random prediction)'),
                   fontsize=title_fontsize)
  plot_2.set_xlabel('Cumulative Bin', fontsize=axis_label_fontsize)
  plot_2.set_ylabel('Precision Uplift', fontsize=axis_label_fontsize)
  plot_2.tick_params(labelsize=axis_tick_fontsize)

  plot_3 = cumulative_bin_metrics.plot.bar(
      x='cumulative_bin_number',
      y='coverage (recall)',
      ax=plots[2],
      color=coverage_bar_color)
  plot_3.set_title(('Coverage (Recall) of the cumulative bins'),
                   fontsize=title_fontsize)
  plot_3.set_xlabel('Cumulative Bin', fontsize=axis_label_fontsize)
  plot_3.set_ylabel('Coverage (Recall)', fontsize=axis_label_fontsize)
  plot_3.tick_params(labelsize=axis_tick_fontsize)

  return plots


def plot_predicted_probabilities(labels: np.ndarray,
                                 probability_predictions: np.ndarray,
                                 colors: Optional[Sequence[str]] = ('b', 'g'),
                                 print_stats: bool = True,
                                 fig_width: Optional[int] = 20,
                                 fig_height: Optional[int] = 15) -> axes.Axes:
  """Plots the distributions of predicted probabilities for each class.

  Args:
    labels: An array of true binary labels represented by 1.0 and 0.0.
    probability_predictions: An array of predicted probabilities between 0.0 and
      1.0.
    colors: Colors for the probability plots.
    print_stats: Flag that whether the stats of probabilities are plotted
    fig_width: Width of the figure.
    fig_height: Height of the figure.

  Returns:
    plots: Class density plots of the predicted probabilities.
  """
  utils.assert_label_values_are_valid(labels)
  utils.assert_prediction_values_are_valid(probability_predictions)
  utils.assert_label_and_prediction_length_match(labels,
                                                 probability_predictions)
  assert len(np.unique(labels)) == len(colors), (
      'number of colors should be the same as number of unique labels.')

  unique_labels = np.sort(np.unique(labels))

  _, plots = pyplot.subplots(figsize=(fig_width, fig_height))
  for color, label in zip(colors, unique_labels):
    index_plot = np.where(labels == label)[0]
    preds_plot = probability_predictions[index_plot]
    label_plot = 'class[%s]' % (str(label))
    if print_stats:
      label_plot += ': mean=%.4f, std=%.4f, median=%.4f' % (
          np.mean(preds_plot), np.std(preds_plot), np.median(preds_plot))
    sns.kdeplot(
        x=preds_plot, shade=True, color=color, label=label_plot, ax=plots)

  pyplot.title('Distribution of predicted probabilities')
  pyplot.legend()
  pyplot.xlabel('Probability')
  pyplot.ylabel('Density')
  pyplot.xlim([0, 1])

  return plots


def plot_roc_curve(labels: np.ndarray,
                   probability_predictions: np.ndarray,
                   print_stats: bool = True,
                   fig_width: Optional[int] = 10,
                   fig_height: Optional[int] = 10,
                   curve_color: Optional[str] = 'blue') -> axes.Axes:
  """Plots the ROC curve for the predictions.

  Args:
    labels: An array of true binary labels represented by 1.0 and 0.0.
    probability_predictions: An array of predicted probabilities between 0.0 and
      1.0.
    print_stats: Flag that whether the AUC is plotted.
    fig_width: Width of the figure.
    fig_height: Height of the figure.
    curve_color: Color of the ROC curve.

  Returns:
      plots: Class density plots of the ROC curve.
  """
  false_positive_rate, ture_positive_rate, _ = sklearn.metrics.roc_curve(
      labels, probability_predictions)
  _, plots = pyplot.subplots(figsize=(fig_width, fig_height))
  pyplot.plot(
      false_positive_rate,
      ture_positive_rate,
      marker='.',
      label='roc',
      color=curve_color)

  pyplot.xlabel('False Positive Rate')
  pyplot.ylabel('True Positive Rate')

  pyplot.legend()
  if print_stats:
    pyplot.title('AUC=%.4f' %
                 sklearn.metrics.roc_auc_score(labels, probability_predictions))

  return plots


def plot_precision_recall_curve(
    labels: np.ndarray,
    probability_predictions: np.ndarray,
    print_stats: bool = True,
    fig_width: Optional[int] = 8,
    fig_height: Optional[int] = 8,
    curve_color: Optional[str] = 'blue') -> axes.Axes:
  """Plots the Precision-Recall curve for the predictions.

  Args:
    labels: An array of true binary labels represented by 1.0 and 0.0.
    probability_predictions: An array of predicted probabilities between 0.0 and
      1.0.
    print_stats: Flag that whether the Average Precision is plotted.
    fig_width: Width of the figure.
    fig_height: Height of the figure.
    curve_color: Color of the Precision-Recall curve.

  Returns:
      plots: Class density plots of the ROC curve.
  """
  utils.assert_label_values_are_valid(labels)
  utils.assert_prediction_values_are_valid(probability_predictions)
  utils.assert_label_and_prediction_length_match(labels,
                                                 probability_predictions)

  precision, recall, _ = sklearn.metrics.precision_recall_curve(
      labels, probability_predictions)
  _, plots = pyplot.subplots(figsize=(fig_width, fig_height))
  pyplot.plot(
      recall,
      precision,
      marker='.',
      label='Precision-Recall',
      color=curve_color)

  pyplot.xlabel('Recall')
  pyplot.ylabel('Precision')

  pyplot.legend()
  if print_stats:
    pyplot.title('Average Precision=%.4f' %
                 sklearn.metrics.average_precision_score(
                     labels, probability_predictions))

  return plots


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
    data: Dataset containing the feature to be plotted with the probability
      predictions columns.
    prediction_column_name: Column name of the probability predictions.
    feature_names: Columns names of the features to be plotted.
    feature_types: Types of the corresponding features to be plotted in order of
      the values in feature_names. Should only contains 'numerical' and
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
