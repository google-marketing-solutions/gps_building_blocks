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
"""Generates plots on the relationships between the featuers and predictions.

Specially useful when diagnosing regression and binary classification ML models
and generating new insights.
"""

from typing import List, Optional, Sequence
from matplotlib import axes
import pandas as pd


def _calc_stats_and_plot_numeric_feature(
    *,  # forces caller to use keyword parameters
    binned_data: pd.DataFrame,
    feature_name: str,
    bar_color: Optional[str] = 'coral',
    fig_width: Optional[int] = 10,
    fig_height: Optional[int] = 5,
    title_fontsize: Optional[int] = 12,
    x_label_fontsize: Optional[int] = 12,
    y_label_fontsize: Optional[int] = 12,
    tick_label_fontsize: Optional[int] = 10) -> axes.Axes:
  """Plots the average of a numerical feature for each prediction bin.

  Args:
    binned_data: Dataset containing the feature to be plotted as a column and
      'bin_number' column representing the bin of the predictions.
    feature_name: Column name of the feature to be plotted.
    bar_color: Color of the bar plots.
    fig_width: Width of the figure.
    fig_height: Height of the figure.
    title_fontsize: Font size of the figure title.
    x_label_fontsize: Font size of the x axis labels.
    y_label_fontsize: Font size of the y axis labels
    tick_label_fontsize: Font size of the x and y axis tick labels.

  Returns:
    plot: A bar plot showing the average value of the feature for each bin.
  """
  # Calculates the average of the feature for each bin.
  bin_stats = binned_data.groupby('bin_number',
                                  as_index=False).agg({feature_name: 'mean'})
  bin_stats.columns = ['bin_number', 'feature_average']

  # Plots the calculated average of the feature for each bin.
  plot = bin_stats.plot.bar(
      x='bin_number',
      y='feature_average',
      color=bar_color,
      figsize=(fig_width, fig_height),
      legend=False)
  plot.set_title(feature_name, fontsize=title_fontsize)
  plot.set_ylabel('Average', fontsize=y_label_fontsize)
  plot.set_xlabel('Bin', fontsize=x_label_fontsize)
  plot.tick_params(labelsize=tick_label_fontsize)

  return plot


def _calc_stats_and_plot_categorical_feature(
    *,  # forces caller to use keyword parameters
    binned_data: pd.DataFrame,
    feature_name: str,
    fig_width: Optional[int] = 10,
    fig_height: Optional[int] = 5,
    title_fontsize: Optional[int] = 12,
    x_label_fontsize: Optional[int] = 12,
    y_label_fontsize: Optional[int] = 12,
    tick_label_fontsize: Optional[int] = 10,
    legend_font_size: Optional[int] = 10) -> axes.Axes:
  """Plots the distribution of a categorical feature for each prediction bin.

  Args:
    binned_data: Dataset containing the feature to be plotted as a column and
      'bin_number' column representing the bin of the predictions.
    feature_name: Column name of the feature to be plotted.
    fig_width: Width of the figure.
    fig_height: Height of the figure.
    title_fontsize: Font size of the figure title.
    x_label_fontsize: Font size of the x axis labels.
    y_label_fontsize: Font size of the y axis labels
    tick_label_fontsize: Font size of the x and y axis tick labels.
    legend_font_size: Font size of the legend.

  Returns:
    plot: A stacked bar plot showing the distribution of categories of the
      feature for each bin.
  """
  # Calculates the distribution (proportion) of each category of the feature
  # within the bin.
  binned_data = binned_data.rename(columns={feature_name: 'categories'})
  binned_data.fillna(value='Missing', inplace=True)

  bin_counts = binned_data.groupby('bin_number', as_index=False).count()
  bin_counts.columns = ['bin_number', 'total_count']

  # Add the 'temp_column' to support the following 'groupby' requiring at
  # least one column not included in `by` parameter.
  binned_data['temp_column'] = 1
  bin_category_counts = binned_data.groupby(['bin_number', 'categories'],
                                            as_index=False).count()
  bin_category_counts.columns = ['bin_number', 'categories', 'count']

  bin_stats = pd.merge(bin_category_counts, bin_counts, on='bin_number')
  bin_stats['percentage'] = ((bin_stats['count'] / bin_stats['total_count']) *
                             100)
  stats_plot_data = pd.pivot_table(
      bin_stats,
      index='bin_number',
      columns='categories',
      values='percentage',
      fill_value=0,
      margins=False)

  # Plots the distribution (percentage) of each category of feature
  # within the bin.
  plot = stats_plot_data.plot.bar(stacked=True, figsize=(fig_width, fig_height))
  plot.set_title(feature_name, fontsize=title_fontsize)
  plot.set_ylabel('Percentage of values', fontsize=y_label_fontsize)
  plot.set_xlabel('Bin', fontsize=x_label_fontsize)
  plot.tick_params(labelsize=tick_label_fontsize)
  plot.legend(prop={'size': legend_font_size})

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
    data: Dataset containing the feature to be plotted with the predictions
      column. The predictions should be numerical such as regression predictions
      or probailitities from a binary classification model.
    prediction_column_name: Column name of the predictions.
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
  assert len(feature_names) == len(feature_types), (
      'feature_names and feature_types should have the same length')
  # Asserts whether the feature_types only contains 'numerical' and
  # 'categorical' as values.
  assert set(feature_types).issubset({
      'numerical', 'categorical'
  }), (("feature_types should contain only 'numerical' and 'categorical' "
        'as values'))
  # Separate the dataset into the bins of the predicted probabilities.
  data = data.sort_values(by=prediction_column_name, ascending=False)
  bin_numbers = pd.qcut(
      data[prediction_column_name], q=number_bins, labels=False)
  # Assign the bin numbers where the bin with the largest predictions having
  # value 1 and so on.
  data['bin_number'] = number_bins - bin_numbers

  plots = []
  for feature_name, feature_type in zip(feature_names, feature_types):
    plot_data = data[['bin_number', feature_name]]

    if feature_type == 'numerical':
      plots.append(
          _calc_stats_and_plot_numeric_feature(
              binned_data=plot_data,
              feature_name=feature_name,
              bar_color=numerical_feature_color,
              fig_width=fig_width,
              fig_height=fig_height,
              title_fontsize=title_fontsize,
              x_label_fontsize=x_label_fontsize,
              y_label_fontsize=y_label_fontsize,
              tick_label_fontsize=tick_label_fontsize))
    else:
      plots.append(
          _calc_stats_and_plot_categorical_feature(
              binned_data=plot_data,
              feature_name=feature_name,
              fig_width=fig_width,
              fig_height=fig_height,
              title_fontsize=title_fontsize,
              x_label_fontsize=x_label_fontsize,
              y_label_fontsize=y_label_fontsize,
              tick_label_fontsize=tick_label_fontsize,
              legend_font_size=legend_font_size))

  return plots
