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

# python3
"""Visualizes the ML features created by the ML Windowing Pipeline.

Calculates statistics from the numerical and categoticals features in the
Features table in BigQuery, generates and outputs plots. These plots can be
used to explore the features to understand the distributions and any anomalies
such as label leakage and inconsistencies over time.

Feature table is created by the FeaturesPipeline of the
ML Windowing Pipeline tool. For more info:
https://github.com/google/gps_building_blocks/tree/master/py/gps_building_blocks/ml/data_prep/ml_windowing_pipeline
"""

from typing import List, Optional, Sequence, Union
from absl import logging
from google.cloud import bigquery
from matplotlib import axes
from matplotlib import pyplot
import numpy as np
import pandas as pd
import seaborn as sns
from gps_building_blocks.ml.data_prep.data_visualizer import viz_utils

# Class FeatureVisualizer utilize these constants to generate  plots and arrange
# them in a single column. By changing the values of these constants will break
# the code.
_NUMERICAL_ROWS_IN_SUBPLOTS_GRID = 3
_NUMERICAL_COLS_IN_SUBPLOTS_GRID = 1
_CATEGORICAL_ROWS_IN_SUBPLOTS_GRID = 3
_CATEGORICAL_COLS_IN_SUBPLOTS_GRID = 1

# Path to the file with sql code to calculate stats from the numerical features
# in the Features table in BigQuery.
_CALC_NUM_FEATURE_STATS_SQL_PATH = viz_utils.get_absolute_path(
    'calc_numerical_feature_stats.sql')
# Path to the file with sql code to calculate stats from the categorical
# features in the Features table in BigQuery.
_CALC_CAT_FEATURE_STATS_SQL_PATH = viz_utils.get_absolute_path(
    'calc_categorical_feature_stats.sql')
# Path to the file with sql code to extract a sample of numerical features
# from the Features table in BigQuery.
_EXTRACT_NUM_FEATURE_SAMPLE_SQL_PATH = viz_utils.get_absolute_path(
    'extract_numerical_features_sample.sql')


class _FeaturePlotStyles:
  """This class encapsulates variables controlling styles of feature plots."""

  def __init__(self,
               fig_width: Optional[int] = 10,
               fig_height: Optional[int] = 30,
               lineplot_title_fontsize: Optional[int] = 15,
               lineplot_legend_fontsize: Optional[int] = 10,
               lineplot_xlabel_fontsize: Optional[int] = 10,
               lineplot_ylabel_fontsize: Optional[int] = 10,
               lineplot_xticklabels_fontsize: Optional[int] = 10,
               lineplot_yticklabels_fontsize: Optional[int] = 10,
               barplot_title_fontsize: Optional[int] = 15,
               barplot_legend_fontsize: Optional[int] = 10,
               barplot_xlabel_fontsize: Optional[int] = 10,
               barplot_ylabel_fontsize: Optional[int] = 10,
               barplot_xticklabels_fontsize: Optional[int] = 10,
               barplot_yticklabels_fontsize: Optional[int] = 10):
    """Initialises parameters.

    Args:
      fig_width: Width of the figure.
      fig_height: Height of the figure.
      lineplot_title_fontsize: Title font size of the line plots.
      lineplot_legend_fontsize: Legend font size of the line plots.
      lineplot_xlabel_fontsize: X-axis label font size of the line plots.
      lineplot_ylabel_fontsize: Y-axis label font size of the line plots.
      lineplot_xticklabels_fontsize: X-axis tick label font size of the line
        plots.
      lineplot_yticklabels_fontsize: Y-axis tick label font size of the line
        plots.
      barplot_title_fontsize: Title font size of the bar plots.
      barplot_legend_fontsize: Legend font size of the bar plots.
      barplot_xlabel_fontsize: X-label font size of the bar plots.
      barplot_ylabel_fontsize: Y-label font size of the bar plots.
      barplot_xticklabels_fontsize: X-label tick label font size of the bar
        plots.
      barplot_yticklabels_fontsize: Y-label tick label font size of the bar
        plots.
    """
    self.fig_width = fig_width
    self.fig_height = fig_height
    self.lineplot_title_fontsize = lineplot_title_fontsize
    self.lineplot_legend_fontsize = lineplot_legend_fontsize
    self.lineplot_xlabel_fontsize = lineplot_xlabel_fontsize
    self.lineplot_ylabel_fontsize = lineplot_ylabel_fontsize
    self.lineplot_xticklabels_fontsize = lineplot_xticklabels_fontsize
    self.lineplot_yticklabels_fontsize = lineplot_yticklabels_fontsize
    self.barplot_title_fontsize = barplot_title_fontsize
    self.barplot_legend_fontsize = barplot_legend_fontsize
    self.barplot_xlabel_fontsize = barplot_xlabel_fontsize
    self.barplot_ylabel_fontsize = barplot_ylabel_fontsize
    self.barplot_xticklabels_fontsize = barplot_xticklabels_fontsize
    self.barplot_yticklabels_fontsize = barplot_yticklabels_fontsize


def _plot_numerical_feature(
    df_data: pd.DataFrame, df_data_sample: pd.DataFrame, feature_name: str,
    positive_class_label: bool, negative_class_label: bool,
    plot_style_params: _FeaturePlotStyles) -> List[axes.Axes]:
  """Plots the statistics of a numerical feature.

  Generates following plots:
  - distribution of values by label
  - average with confidence interval for label=True by snapshot_date
  - average with confidence interval for label=False by snapshot_date

  Args:
    df_data: data to plot containing snapshot_date, label, record_count,
      prop_missing, prop_non_num, average and stddev columns.
    df_data_sample: data to plot containing columns corresponding to features
      and label
    feature_name: Name of the feature.
    positive_class_label: label for positive class
    negative_class_label: label for negative class
    plot_style_params: Plot style parameters.

  Returns:
    plots: A list of Axes containing 4 plots.
  """
  logging.info('Plotting numerical feature %s', feature_name)
  _, plots = pyplot.subplots(
      nrows=_NUMERICAL_ROWS_IN_SUBPLOTS_GRID,
      ncols=_NUMERICAL_COLS_IN_SUBPLOTS_GRID,
      figsize=(plot_style_params.fig_width, plot_style_params.fig_height))

  df_data = df_data.sort_values(by='snapshot_date', ascending=True)
  # Calculate 95% confidence interval to plot error bars
  # indicating estimated range of values for average.
  df_data.loc[:, 'ci'] = (1.96 * df_data['stddev'] /
                          np.sqrt(df_data['record_count']))

  density_plot_common_params = {
      'plot_data': df_data_sample,
      'label_variable': 'label',
      'title_fontsize': plot_style_params.lineplot_title_fontsize,
      'class1_label': positive_class_label,
      'class2_label': negative_class_label,
      'axes': plots
  }

  logging.info('Plotting conditional feature distribution.')
  viz_utils.plot_class_densities(
      plot_variable=feature_name,
      title=f'Class distribution of {feature_name}',
      subplot_index=0,
      **density_plot_common_params)

  # Daily Average of feature per Snapshot for positive label
  pos_data = df_data[df_data['label'] == positive_class_label]

  common_lineplot_params = {
      'axes': plots,
      'title_fontsize': plot_style_params.lineplot_title_fontsize,
      'xticklabels_fontsize': plot_style_params.lineplot_xticklabels_fontsize,
      'yticklabels_fontsize': plot_style_params.lineplot_yticklabels_fontsize
  }
  title_text = 'Daily Average per Snapshot for label'

  viz_utils.plot_line(
      plot_data=pos_data,
      x_variable='snapshot_date',
      y_variable='average',
      line_color='green',
      title=f'{feature_name} - {title_text} = {positive_class_label}',
      subplot_index=1,
      **common_lineplot_params)

  plots[1].errorbar(
      x=pos_data['snapshot_date'],
      y=pos_data['average'],
      yerr=pos_data['ci'])

  # Daily Average per Snapshot for label = False
  neg_data = df_data[df_data['label'] == negative_class_label]

  viz_utils.plot_line(
      plot_data=neg_data,
      x_variable='snapshot_date',
      y_variable='average',
      line_color='blue',
      title=f'{feature_name} - {title_text} = {negative_class_label}',
      subplot_index=2,
      **common_lineplot_params)

  plots[2].errorbar(
      x=neg_data['snapshot_date'],
      y=neg_data['average'],
      yerr=neg_data['ci'])
  return plots


def _plot_categorical_feature(
    df_data: pd.DataFrame, feature_name: str,
    plot_style_params: _FeaturePlotStyles) -> List[axes.Axes]:
  """Plots the statistics of a categorical feature.

  Generates following plots:
  - Snapshot distribution of proportion of values by label
  - Proportion of values by snapshot_date for label=True
  - Proportion of values by snapshot_date for label=True

  Args:
    df_data: data to plot containing : snapshot_date, label, record_count,
      prop_missing, prop_non_num, average, stddev columns.
    feature_name: Name of the feature.
    plot_style_params: Plot style parameters.

  Returns:
     plots: A list of Axes containing 4 plots.
  """
  logging.info('Plotting numerical feature %s', feature_name)

  _, plots = pyplot.subplots(
      nrows=_CATEGORICAL_ROWS_IN_SUBPLOTS_GRID,
      ncols=_CATEGORICAL_COLS_IN_SUBPLOTS_GRID,
      figsize=(plot_style_params.fig_width, plot_style_params.fig_height))

  # Processing types for filtering and sorting.
  df_data.loc[:, 'label'] = df_data['label'].astype(str)

  # Aggregating dataframe on date level to get data for the first chart.
  cols_value_count = ['feature', 'value', 'label']
  df_value_count = df_data.groupby(cols_value_count)[['count']].sum()

  cols_total_count = ['feature', 'label']
  df_total_count = df_data.groupby(cols_total_count)[['count']].sum()
  df_total_count = df_total_count.rename(columns={'count': 'total_count'})

  # Joining total counts and calculating proportions.
  df_value_proportions = df_value_count.merge(
      right=df_total_count, on=cols_total_count)
  df_value_proportions['percentage'] = (
      df_value_proportions['count'] / df_value_proportions['total_count']) * 100

  # Dataframe for label True.
  pos = df_data[df_data['label'] == 'True']
  pos = pos.sort_values(['snapshot_date', 'feature'], ascending=True)
  pos_pivoted = pos.pivot(
      index='snapshot_date', columns='value', values='percentage')

  # Dataframe for label False.
  neg = df_data[df_data['label'] == 'True']
  neg = neg.sort_values(['snapshot_date', 'feature'], ascending=True)
  neg_pivoted = neg.pivot(
      index='snapshot_date', columns='value', values='percentage')

  # Class conditional proportions for each category.
  plot1 = sns.barplot(
      x='percentage', y='value', data=df_data, ax=plots[0], hue='label')
  plot1.set_title(
      'Distribution of ' + feature_name,
      fontsize=plot_style_params.barplot_title_fontsize)
  plot1.set_xlabel('%', fontsize=plot_style_params.barplot_xlabel_fontsize)
  plot1.set_ylabel('Value', fontsize=plot_style_params.barplot_ylabel_fontsize)
  plot1.tick_params(labelsize=plot_style_params.barplot_xticklabels_fontsize)

  # Plot positive class.
  plot2 = pos_pivoted.plot.bar(stacked=True, ax=plots[1], rot=45)
  plot2.set_title(
      'Snapshot Distribution of ' + feature_name + ' for label = True',
      fontsize=plot_style_params.barplot_title_fontsize)
  plot2.set_ylabel('%', fontsize=plot_style_params.barplot_ylabel_fontsize)
  plot2.set_xlabel(
      'Snapshot Date', fontsize=plot_style_params.barplot_xticklabels_fontsize)

  # Plot negative class.
  plot3 = neg_pivoted.plot.bar(stacked=True, ax=plots[2], rot=45)
  plot3.set_title(
      'Snapshot Distribution of ' + feature_name + ' for label = False',
      fontsize=plot_style_params.barplot_title_fontsize)
  plot3.set_ylabel('%', fontsize=plot_style_params.barplot_ylabel_fontsize)
  plot3.set_xlabel(
      'Snapshot Date', fontsize=plot_style_params.barplot_xticklabels_fontsize)
  return plots


class FeatureVisualizer(object):
  """This class provides methods to visualize the ML features.

  Features table is created by the GenerateFeaturesPipeline of
  MLDataWindowingPipeline.
  """

  def __init__(self, bq_client: bigquery.client.Client,
               features_table_path: str, numerical_features: Sequence[str],
               categorical_features: Sequence[str], label_column: str,
               positive_class_label: Union[str, bool, int],
               negative_class_label: Union[str, bool, int],
               num_pos_instances: int, num_neg_instances: int) -> None:
    """Initialises parameters.

    Args:
      bq_client: Connection object to the Bigquery account.
      features_table_path: Full path to the BigQuery Features table. example:
        'project_id.dataset.features_table
      numerical_features: List of numerical feature names to calculate
        statistics for.
      categorical_features: List of categorical feature names to calculate
        statistics for.
      label_column: Name of the label column of the Instance table.
      positive_class_label: Label value representing the positive class
        instances.
      negative_class_label: Label value representing the negative class
        instances.
      num_pos_instances: Number of positive instances to randomly select for
        numerical feature visualization.
      num_neg_instances: Number of negative instances to randomly select for
        numerical feature visualization.
    """
    self._bq_client = bq_client
    self._features_table_path = features_table_path
    self._numerical_feature_list = list(numerical_features)
    self._categorical_feature_list = list(categorical_features)
    self._label_column = label_column
    self._positive_class_label = positive_class_label
    self._negative_class_label = negative_class_label
    self._num_pos_instances = num_pos_instances
    self._num_neg_instances = num_neg_instances

  def _create_struct_column_list_sql(self, column_list: Sequence[str]) -> str:
    """Creates an sql segment containing a list of STRUCT of columns.

    The resulted sql segment contains each column in the input column list
    in the following format: STRUCT('column' AS feature, column AS value).

    Args:
      column_list: a list containing the selected column names.

    Returns:
      results: sql code segment.
    """
    sql_segment = ', '.join(
        f"STRUCT('{column}' AS feature, {column} AS value)"
        for column in column_list
        )

    return sql_segment

  def _create_column_list_sql(self, column_list: Sequence[str]) -> str:
    """Creates an sql segment containing a list of comma separated columns.

    Args:
      column_list: a list containing the selected column names.

    Returns:
       results: sql code segment.
    """
    sql_segment = column_list[0]
    for column in column_list[1:]:
      sql_segment = f'{sql_segment}, {column}'

    return sql_segment

  def _calc_numerical_feature_stats(self) -> pd.DataFrame:
    """Calculates the statistics from selected numerical features.

    Returns:
      results: Calculated statistics.
    """
    logging.info('Calculating statistics from numerical features.')
    logging.info('Creating the sql code.')
    sql_segment = self._create_struct_column_list_sql(
        self._numerical_feature_list)
    query_params = {
        'bq_features_table': self._features_table_path,
        'sql_code_segment': sql_segment
    }
    sql_query = viz_utils.patch_sql(_CALC_NUM_FEATURE_STATS_SQL_PATH,
                                    query_params)
    logging.info('Finished creating the sql code.')

    logging.info('Executing the sql code.')
    results = viz_utils.execute_sql(self._bq_client, sql_query)
    logging.info('Finished executing the sql code.')

    return results

  def _extract_numerical_feature_sample(self) -> pd.DataFrame:
    """Extracts a random sample of values from selected numerical features.

    Returns:
      results: Extracted values as a DataFrame.
    """
    logging.info('Extracting a random sample of numerical features.')
    logging.info('Creating the sql code.')
    sql_segment = self._create_column_list_sql(
        self._numerical_feature_list)
    query_params = {
        'bq_features_table': self._features_table_path,
        'label_column': self._label_column,
        'positive_class_label': self._positive_class_label,
        'negative_class_label': self._negative_class_label,
        'num_pos_instances': self._num_pos_instances,
        'num_neg_instances': self._num_neg_instances,
        'column_list_sql': sql_segment
    }
    sql_query = viz_utils.patch_sql(_EXTRACT_NUM_FEATURE_SAMPLE_SQL_PATH,
                                    query_params)
    logging.info('Finished creating the sql code.')

    logging.info('Executing the sql code.')
    results = viz_utils.execute_sql(self._bq_client, sql_query)
    logging.info('Finished executing the sql code.')

    return results

  def _calc_categorical_feature_stats(self) -> pd.DataFrame:
    """Calculates the statistics from selected categorical features.

    Returns:
      results: Calculated statistics.
    """
    logging.info('Calculating statistics from categorical features.')
    logging.info('Creating the sql code.')
    sql_segment = self._create_struct_column_list_sql(
        self._categorical_feature_list)
    query_params = {
        'bq_features_table': self._features_table_path,
        'sql_code_segment': sql_segment
    }
    sql_query = viz_utils.patch_sql(_CALC_CAT_FEATURE_STATS_SQL_PATH,
                                    query_params)
    logging.info('Finished creating the sql code.')

    logging.info('Executing the sql code.')
    results = viz_utils.execute_sql(self._bq_client, sql_query)
    logging.info('Finished executing the sql code.')

    return results

  def plot_features(
      self,
      fig_width: Optional[int] = 30,
      fig_height: Optional[int] = 25,
      lineplot_title_fontsize: Optional[int] = 12,
      lineplot_legend_fontsize: Optional[int] = 10,
      lineplot_xlabel_fontsize: Optional[int] = 10,
      lineplot_ylabel_fontsize: Optional[int] = 10,
      lineplot_xticklabels_fontsize: Optional[int] = 10,
      lineplot_yticklabels_fontsize: Optional[int] = 10,
      barplot_title_fontsize: Optional[int] = 12,
      barplot_legend_fontsize: Optional[int] = 10,
      barplot_xlabel_fontsize: Optional[int] = 10,
      barplot_ylabel_fontsize: Optional[int] = 10,
      barplot_xticklabels_fontsize: Optional[int] = 8,
      barplot_yticklabels_fontsize: Optional[int] = 10
  ) -> List[List[axes.Axes]]:
    """Creates plots for numerical and categorical features.

    Before plotting executes sql statements to return stats
    for numerical and categorical features.
    Args:
      fig_width: Width of the figure.
      fig_height: Height of the figure.
      lineplot_title_fontsize: Title font size of the line plots.
      lineplot_legend_fontsize: Legend font size of the line plots.
      lineplot_xlabel_fontsize: X-axis label font size of the line plots.
      lineplot_ylabel_fontsize: Y-axis label font size of the line plots.
      lineplot_xticklabels_fontsize: X-axis tick label font size of the line
        plots.
      lineplot_yticklabels_fontsize: Y-axis tick label font size of the line
        plots.
      barplot_title_fontsize: Title font size of the bar plots.
      barplot_legend_fontsize: Legend font size of the bar plots.
      barplot_xlabel_fontsize: X-label font size of the bar plots.
      barplot_ylabel_fontsize: Y-label font size of the bar plots.
      barplot_xticklabels_fontsize: X-axis tick label font size of the bar
        plots.
      barplot_yticklabels_fontsize: Y-axis tick label font size of the bar
        plots.

    Returns:
      all_plots: all the plots generated for the selected features.
    """
    plot_style_params = _FeaturePlotStyles(
        fig_width=fig_width,
        fig_height=fig_height,
        lineplot_title_fontsize=lineplot_title_fontsize,
        lineplot_legend_fontsize=lineplot_legend_fontsize,
        lineplot_xlabel_fontsize=lineplot_xlabel_fontsize,
        lineplot_ylabel_fontsize=lineplot_ylabel_fontsize,
        lineplot_xticklabels_fontsize=lineplot_xticklabels_fontsize,
        lineplot_yticklabels_fontsize=lineplot_yticklabels_fontsize,
        barplot_title_fontsize=barplot_title_fontsize,
        barplot_legend_fontsize=barplot_legend_fontsize,
        barplot_xlabel_fontsize=barplot_xlabel_fontsize,
        barplot_ylabel_fontsize=barplot_ylabel_fontsize,
        barplot_xticklabels_fontsize=barplot_xticklabels_fontsize,
        barplot_yticklabels_fontsize=barplot_yticklabels_fontsize)

    numerical_feature_stats = self._calc_numerical_feature_stats()
    numerical_feature_sample = self._extract_numerical_feature_sample()
    categorical_feature_stats = self._calc_categorical_feature_stats()

    numerical_feature_stats.loc[:, 'snapshot_date'] = pd.to_datetime(
        numerical_feature_stats['snapshot_date'])
    categorical_feature_stats.loc[:, 'snapshot_date'] = pd.to_datetime(
        categorical_feature_stats['snapshot_date']).dt.date.astype(str)

    all_plots = []

    logging.info('Plotting numerical features.')
    for feature_name in self._numerical_feature_list:
      num_plot_data = numerical_feature_stats[numerical_feature_stats['feature']
                                              == feature_name]
      cols = [feature_name, 'label']
      num_plot_data_sample = numerical_feature_sample[cols]
      all_plots.append(
          _plot_numerical_feature(num_plot_data, num_plot_data_sample,
                                  feature_name, self._positive_class_label,
                                  self._negative_class_label,
                                  plot_style_params))

    logging.info('Plotting categorical features.')
    for feature_name in self._categorical_feature_list:
      cat_plot_data = categorical_feature_stats[
          categorical_feature_stats['feature'] == feature_name]
      all_plots.append(
          _plot_categorical_feature(cat_plot_data, feature_name,
                                    plot_style_params))

    return all_plots
