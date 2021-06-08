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
import warnings
from absl import logging
from google.cloud import bigquery
from matplotlib import axes
from matplotlib import pyplot
import numpy as np
import pandas as pd
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

# Type of the label values
LabelType = Union[str, bool, int]

warnings.filterwarnings('ignore')


class _FeaturePlotStyles:
  """This class encapsulates variables controlling styles of feature plots."""

  def __init__(self,
               fig_width: Optional[int] = 10,
               fig_height: Optional[int] = 30,
               title_fontsize: Optional[int] = 15,
               legend_fontsize: Optional[int] = 10,
               xlabel_fontsize: Optional[int] = 10,
               ylabel_fontsize: Optional[int] = 10,
               xticklabels_fontsize: Optional[int] = 10,
               yticklabels_fontsize: Optional[int] = 10):
    """Initialises parameters.

    Args:
      fig_width: Width of the figure.
      fig_height: Height of the figure.
      title_fontsize: Title font size.
      legend_fontsize: Legend font size.
      xlabel_fontsize: X-axis label font size.
      ylabel_fontsize: Y-axis label font size.
      xticklabels_fontsize: X-axis tick label font size.
      yticklabels_fontsize: Y-axis tick label font size.
    """
    self.fig_width = fig_width
    self.fig_height = fig_height
    self.title_fontsize = title_fontsize
    self.legend_fontsize = legend_fontsize
    self.xlabel_fontsize = xlabel_fontsize
    self.ylabel_fontsize = ylabel_fontsize
    self.xticklabels_fontsize = xticklabels_fontsize
    self.yticklabels_fontsize = yticklabels_fontsize


def _plot_numerical_feature(
    df_data: pd.DataFrame, df_data_sample: pd.DataFrame, feature_name: str,
    positive_class_label: LabelType, negative_class_label: LabelType,
    plot_style_params: _FeaturePlotStyles) -> List[axes.Axes]:
  """Plots the statistics of a numerical feature.

  Generates following plots:
  - distribution of values by label
  - average with confidence interval for positive instances by snapshot_date
  - average with confidence interval for negative instances by snapshot_date

  Args:
    df_data: data to plot containing snapshot_date, label, record_count,
      prop_missing, prop_non_num, average and stddev columns.
    df_data_sample: data to plot containing columns corresponding to features
      and label.
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

  # Plot class conditional distribution of the feature
  plot_data = df_data_sample.pivot(columns='label', values=feature_name)
  box_plot = plot_data.plot.box(ax=plots[0], vert=False, grid=True)
  box_plot.yaxis.grid(True, linestyle='dashed')
  box_plot.set_title(
      label=f'Distribution of [{feature_name}]',
      fontsize=plot_style_params.title_fontsize)
  box_plot.set_xlabel(
      xlabel='values', fontsize=plot_style_params.xlabel_fontsize)
  box_plot.set_ylabel(
      ylabel='label', fontsize=plot_style_params.ylabel_fontsize)
  box_plot.tick_params(
      axis='x', which='both', labelsize=plot_style_params.xticklabels_fontsize)
  box_plot.tick_params(
      axis='y', which='both', labelsize=plot_style_params.yticklabels_fontsize)

  df_data = df_data.sort_values(by='snapshot_date', ascending=True)
  # Calculate 95% confidence intervals to plot error bars
  # indicating estimated range of values for average.
  df_data.loc[:, 'ci'] = (1.96 * df_data['stddev'] /
                          np.sqrt(df_data['record_count']))

  # Daily Average of feature per Snapshot for positive label
  pos_data = df_data[df_data['label'] == positive_class_label]

  common_lineplot_params = {
      'axes': plots,
      'title_fontsize': plot_style_params.title_fontsize,
      'xlabel_fontsize': plot_style_params.xlabel_fontsize,
      'ylabel_fontsize': plot_style_params.ylabel_fontsize,
      'xticklabels_fontsize': plot_style_params.xticklabels_fontsize,
      'yticklabels_fontsize': plot_style_params.yticklabels_fontsize
  }
  title_text = 'Daily Average per Snapshot for label'

  viz_utils.plot_line(
      plot_data=pos_data,
      x_variable='snapshot_date',
      y_variable='average',
      line_color='limegreen',
      title=f'[{feature_name}] | {title_text} = {positive_class_label}',
      subplot_index=1,
      **common_lineplot_params)

  # Adding error bars to subplot.
  plots[1].errorbar(
      x=pos_data['snapshot_date'],
      y=pos_data['average'],
      yerr=pos_data['ci'],
      ecolor='limegreen',
      linestyle='--',
      capsize=5,
      alpha=0.5)

  # Daily Average of feature per Snapshot for negative label
  neg_data = df_data[df_data['label'] == negative_class_label]

  viz_utils.plot_line(
      plot_data=neg_data,
      x_variable='snapshot_date',
      y_variable='average',
      line_color='cornflowerblue',
      title=f'[{feature_name}] | {title_text} = {negative_class_label}',
      subplot_index=2,
      **common_lineplot_params)

  # Adding error bars to subplot.
  plots[2].errorbar(
      x=neg_data['snapshot_date'],
      y=neg_data['average'],
      yerr=neg_data['ci'],
      ecolor='cornflowerblue',
      linestyle='--',
      capsize=5,
      alpha=0.5)

  return plots


def _plot_categorical_feature(
    df_data: pd.DataFrame, feature_name: str, positive_class_label: LabelType,
    negative_class_label: LabelType,
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
    positive_class_label: label for positive class
    negative_class_label: label for negative class
    plot_style_params: Plot style parameters.

  Returns:
     plots: A list of Axes containing 4 plots.
  """
  logging.info('Plotting categorical feature %s', feature_name)

  _, plots = pyplot.subplots(
      nrows=_CATEGORICAL_ROWS_IN_SUBPLOTS_GRID,
      ncols=_CATEGORICAL_COLS_IN_SUBPLOTS_GRID,
      figsize=(plot_style_params.fig_width, plot_style_params.fig_height))

  # Aggregating dataframe on date level to get data for the category
  # distribution plot.
  df_value_count = df_data.groupby(['label',
                                    'value'])[['count']].sum().reset_index()

  df_total_count = df_data.groupby('label')[['count']].sum().reset_index()
  df_total_count = df_total_count.rename(columns={'count': 'total_count'})

  # Joining total counts and calculating proportions.
  df_value_proportions = df_value_count.merge(df_total_count, on='label')
  df_value_proportions['percentage'] = (
      df_value_proportions['count'] / df_value_proportions['total_count']) * 100

  common_barplot_params = {
      'axes': plots,
      'title_fontsize': plot_style_params.title_fontsize,
      'xlabel_fontsize': plot_style_params.xlabel_fontsize,
      'ylabel_fontsize': plot_style_params.ylabel_fontsize,
      'xticklabels_fontsize': plot_style_params.xticklabels_fontsize,
      'yticklabels_fontsize': plot_style_params.yticklabels_fontsize
  }

  viz_utils.plot_bar(
      plot_data=df_value_proportions,
      x_variable='value',
      y_variable='percentage',
      group_variable='label',
      title=f'Distribution of [{feature_name}]',
      subplot_index=0,
      **common_barplot_params)

  # Dataframe for positive instances.
  pos_data = df_data[df_data['label'] == positive_class_label]
  pos_data = pos_data.sort_values(['snapshot_date', 'feature'], ascending=True)

  # Plot for positive instances.
  pos_plot_title = (f'Snapshot-level Distribution of [{feature_name}] for '
                    'label = {positive_class_label}')
  viz_utils.plot_bar(
      plot_data=pos_data,
      x_variable='snapshot_date',
      y_variable='percentage',
      group_variable='value',
      stacked_bars=True,
      title=pos_plot_title,
      subplot_index=1,
      xticklabels_rotation=45,
      x_label='',
      **common_barplot_params)

  # Dataframe for negative instances.
  neg_data = df_data[df_data['label'] == negative_class_label]
  neg_data = neg_data.sort_values(['snapshot_date', 'feature'], ascending=True)

  # Plot for negative instances.
  neg_plot_title = (f'Snapshot-level Distribution of [{feature_name}] for '
                    'label = {negative_class_label}')
  viz_utils.plot_bar(
      plot_data=neg_data,
      x_variable='snapshot_date',
      y_variable='percentage',
      group_variable='value',
      stacked_bars=True,
      title=neg_plot_title,
      subplot_index=2,
      xticklabels_rotation=45,
      **common_barplot_params,
  )

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
      fig_height: Optional[int] = 26,
      title_fontsize: Optional[int] = 18,
      legend_fontsize: Optional[int] = 12,
      xlabel_fontsize: Optional[int] = 12,
      ylabel_fontsize: Optional[int] = 15,
      xticklabels_fontsize: Optional[int] = 12,
      yticklabels_fontsize: Optional[int] = 12) -> List[List[axes.Axes]]:
    """Creates plots for numerical and categorical features.

    Before plotting executes sql statements to return stats
    for numerical and categorical features.
    Args:
      fig_width: Width of the figure.
      fig_height: Height of the figure.
      title_fontsize: Title font size.
      legend_fontsize: Legend font size.
      xlabel_fontsize: X-axis label font size.
      ylabel_fontsize: Y-axis label font size.
      xticklabels_fontsize: X-axis tick label font size.
      yticklabels_fontsize: Y-axis tick label font size.

    Returns:
      all_plots: all the plots generated for the selected features.
    """
    plot_style_params = _FeaturePlotStyles(
        fig_width=fig_width,
        fig_height=fig_height,
        title_fontsize=title_fontsize,
        legend_fontsize=legend_fontsize,
        xlabel_fontsize=xlabel_fontsize,
        ylabel_fontsize=ylabel_fontsize,
        xticklabels_fontsize=xticklabels_fontsize,
        yticklabels_fontsize=yticklabels_fontsize)

    numerical_feature_stats = self._calc_numerical_feature_stats()
    numerical_feature_sample = self._extract_numerical_feature_sample()
    categorical_feature_stats = self._calc_categorical_feature_stats()

    numerical_feature_stats.loc[:, 'snapshot_date'] = pd.to_datetime(
        numerical_feature_stats['snapshot_date']).dt.date.astype(str)
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
                                    self._positive_class_label,
                                    self._negative_class_label,
                                    plot_style_params))

    return all_plots
