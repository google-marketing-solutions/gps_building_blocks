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
used to explore the features to understand their distributions, relationships
with the label, any anomalies (such as label leakage) and inconsistencies over
time.

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

# Class FeatureVisualizer utilizes the following constants to generate plots
# and arrange them in a single column in the pdf outputs. By changing the
# values of these constants will break the code
# Number of columns to arrange the plots
_COLS_IN_SUBPLOTS_GRID = 1
# Number of plots for a numerical feature when the label is binary
_NO_PLOTS_NUM_FEATURE_BINARY_LABEL = 3
# Number of plots for a numerical feature when the label is numerical
_NO_PLOTS_NUM_FEATURE_NUM_LABEL = 2
# Number of plots for a categorical feature when the label is binary
_NO_PLOTS_CAT_FEATURE_BINARY_LABEL = 3
# Number of plots for a categorical feature when the label is numerical
_NO_PLOTS_CAT_FEATURE_NUM_LABEL = 2

# Path to sql files to calculate stats from the features and label
# in the Features table in BigQuery when the label is binary.
_BINARY_LABEL_SQL_FILES = {
    'calc_num_feature_stats': viz_utils.get_absolute_path(
        'calc_num_feature_stats_binary_label.sql'),
    'calc_cat_feature_stats': viz_utils.get_absolute_path(
        'calc_cat_feature_stats_binary_label.sql'),
    'extract_num_feature': viz_utils.get_absolute_path(
        'extract_num_features_sample_binary_label.sql'),
}

# Path to sql files to calculate stats from the features and label
# in the Features table in BigQuery when the label is numerical.
_NUMERICAL_LABEL_SQL_FILES = {
    'calc_num_feature_stats': viz_utils.get_absolute_path(
        'calc_num_feature_stats_numerical_label.sql'),
    'calc_cat_feature_stats': viz_utils.get_absolute_path(
        'calc_cat_feature_stats_numerical_label.sql'),
    'extract_num_feature': viz_utils.get_absolute_path(
        'extract_num_features_sample_numerical_label.sql'),
    'calc_num_label_stats': viz_utils.get_absolute_path(
        'calc_num_label_stats_cat_feature.sql')
}

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


def _plot_numerical_feature_binary_label(
    df_data: pd.DataFrame, df_data_sample: pd.DataFrame, feature_name: str,
    label_column: str, positive_class_label: LabelType,
    negative_class_label: LabelType,
    plot_style_params: _FeaturePlotStyles) -> List[axes.Axes]:
  """Plots the statistics of a numerical feature when the label is binary.

  Generates following plots of the feature:
  - distribution of values (box plots) by label.
  - distribution of values (box plots) for positive instances by snapshot_date.
  - distribution of values (box plots) for negative instances by snapshot_date.

  Args:
    df_data: Plot data containing the following columns: snapshot_date, feature,
      label, record_count, prop_missing, prop_non_num, mean, stddev, med, q1,
      q3, whislo and whishi.
    df_data_sample: Plot data containing containing following columns: feature
      and label.
    feature_name: Name of the feature.
    label_column: Name of the label column.
    positive_class_label: label for positive class.
    negative_class_label: label for negative class.
    plot_style_params: Plot style parameters.

  Returns:
    plots: A list of Axes containing 3 plots.
  """

  logging.info('Plotting numerical feature %s', feature_name)
  _, plots = pyplot.subplots(
      nrows=_NO_PLOTS_NUM_FEATURE_BINARY_LABEL,
      ncols=_COLS_IN_SUBPLOTS_GRID,
      figsize=(plot_style_params.fig_width, plot_style_params.fig_height))

  # Plot class conditional distribution of the feature (box plots)
  plot_data = df_data_sample.pivot(columns=label_column, values=feature_name)

  logging.info('Plotting class-conditional feature distribution.')
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

  # Plot snapshot-level distribution of the feature (box plots)
  snapshot_box_plot_common_params = {
      'x_variable': 'snapshot_date',
      'axes': plots,
      'title_fontsize': plot_style_params.title_fontsize,
      'xlabel_fontsize': plot_style_params.xlabel_fontsize,
      'ylabel_fontsize': plot_style_params.ylabel_fontsize,
      'xticklabels_fontsize': plot_style_params.xticklabels_fontsize,
      'yticklabels_fontsize': plot_style_params.yticklabels_fontsize,
      'xticklabels_rotation': 45
  }

  # For positive instances
  pos_instance_stats = df_data[df_data[label_column] == positive_class_label]
  pos_instance_stats = pos_instance_stats.sort_values('snapshot_date')

  pos_plot_title = (f'Snapshot-level distribution of [{feature_name}] for '
                    f'label = {positive_class_label}')
  logging.info(
      'Plotting snapshot-level feature distribution for positive instances.')
  viz_utils.plot_box(
      plot_data=pos_instance_stats,
      title=pos_plot_title,
      subplot_index=1,
      **snapshot_box_plot_common_params)

  # For negative instances
  neg_instance_stats = df_data[df_data[label_column] == negative_class_label]
  neg_instance_stats = neg_instance_stats.sort_values('snapshot_date')

  neg_plot_title = (f'Snapshot-level distribution of [{feature_name}] for '
                    f'label = {negative_class_label}')

  logging.info(
      'Plotting snapshot-level feature distribution for negative instances.')
  viz_utils.plot_box(
      plot_data=neg_instance_stats,
      title=neg_plot_title,
      x_label='Snapshot date',
      subplot_index=2,
      **snapshot_box_plot_common_params)

  return plots


def _plot_numerical_feature_numerical_label(
    df_data: pd.DataFrame, df_data_sample: pd.DataFrame, feature_name: str,
    label_column: str,
    plot_style_params: _FeaturePlotStyles) -> List[axes.Axes]:
  """Plots the statistics of a numerical feature when the label is numerical.

  Generates following plots:
  - scatter plot of the feature vs label with correlation value.
  - distribution of values (box plots) by snapshot_date.

  Args:
    df_data: Plot data containing the following columns: snapshot_date, feature,
      record_count, prop_missing, prop_non_num, mean, stddev, med, q1, q3,
      whislo and whishi.
    df_data_sample: Plot data containing the following columns: feature and
      label.
    feature_name: Name of the feature.
    label_column: Name of the label column.
    plot_style_params: Plot style parameters.

  Returns:
    plots: A list of Axes containing 2 plots.
  """

  logging.info('Plotting numerical feature %s', feature_name)
  _, plots = pyplot.subplots(
      nrows=_NO_PLOTS_NUM_FEATURE_NUM_LABEL,
      ncols=_COLS_IN_SUBPLOTS_GRID,
      figsize=(plot_style_params.fig_width, plot_style_params.fig_height))

  # Cap outliers of the label
  label_p1 = np.quantile(df_data_sample[label_column], 0.01)  # 1st percentile
  label_p99 = np.quantile(df_data_sample[label_column], 0.99)  # 99th percentile
  df_data_sample.loc[df_data_sample[label_column] < label_p1,
                     label_column] = label_p1
  df_data_sample.loc[df_data_sample[label_column] > label_p99,
                     label_column] = label_p99

  # Cap outliers of the feature
  feature_p1 = np.quantile(df_data_sample[feature_name], 0.01)  # 1st percentile
  feature_p99 = np.quantile(df_data_sample[feature_name], 0.99)  # 99th perc.
  df_data_sample.loc[df_data_sample[feature_name] < feature_p1,
                     feature_name] = feature_p1
  df_data_sample.loc[df_data_sample[feature_name] > feature_p99,
                     feature_name] = feature_p99

  # Calculating correlation between feature and label
  correlation = np.corrcoef(df_data_sample[label_column],
                            df_data_sample[feature_name])[0][1]

  # Scatter plot between label and and feature
  scatter_title = (f'Scatter plot of the Label vs [{feature_name}] | '
                   f' correlation = {round(correlation, 2)}')
  scatter_plot = df_data_sample.plot.scatter(
      x=feature_name, y=label_column, ax=plots[0])
  scatter_plot.set_title(
      label=scatter_title, fontsize=plot_style_params.title_fontsize)
  scatter_plot.set_xlabel(
      xlabel=feature_name, fontsize=plot_style_params.xlabel_fontsize)
  scatter_plot.set_ylabel(
      ylabel='Label', fontsize=plot_style_params.ylabel_fontsize)
  scatter_plot.tick_params(
      axis='x', which='both', labelsize=plot_style_params.xticklabels_fontsize)
  scatter_plot.tick_params(
      axis='y', which='both', labelsize=plot_style_params.yticklabels_fontsize)

  # Plot snapshot-level feature distribution
  logging.info('Plotting snapshot-level feature distribution.')
  viz_utils.plot_box(
      plot_data=df_data,
      title=f'Snapshot-level distribution of [{feature_name}]',
      axes=plots,
      subplot_index=1,
      x_variable='snapshot_date',
      x_label='Snapshot date',
      title_fontsize=plot_style_params.title_fontsize,
      xlabel_fontsize=plot_style_params.xlabel_fontsize,
      ylabel_fontsize=plot_style_params.ylabel_fontsize,
      xticklabels_fontsize=plot_style_params.xticklabels_fontsize,
      yticklabels_fontsize=plot_style_params.yticklabels_fontsize,
      xticklabels_rotation=45)

  return plots


def _plot_categorical_feature_binary_label(
    df_data: pd.DataFrame, feature_name: str, label_column: str,
    positive_class_label: LabelType, negative_class_label: LabelType,
    plot_style_params: _FeaturePlotStyles) -> List[axes.Axes]:
  """Plots the statistics of a categorical feature when label is binary.

  Generates following plots of the feature:
  - distribution of values (bor plots) by label.
  - distribution of values (stacked bor plots) for positive instances by
      snapshot_date.
  - distribution of values (stacked bor plots) for negative instances by
      snapshot_date.

  Args:
    df_data: plot data containing the following columns: snapshot_date, label,
      record_count, prop_missing, prop_non_num, average, stddev columns.
    feature_name: Name of the feature.
    label_column: Name of the label column.
    positive_class_label: label for positive class
    negative_class_label: label for negative class
    plot_style_params: Plot style parameters.

  Returns:
     plots: A list of Axes containing 3 plots.
  """
  logging.info('Plotting categorical feature %s', feature_name)

  _, plots = pyplot.subplots(
      nrows=_NO_PLOTS_CAT_FEATURE_BINARY_LABEL,
      ncols=_COLS_IN_SUBPLOTS_GRID,
      figsize=(plot_style_params.fig_width, plot_style_params.fig_height))

  # Aggregating dataframe on date level to get data for the category
  # distribution plot.
  df_value_count = df_data.groupby([label_column,
                                    'value'])[['count']].sum().reset_index()

  df_total_count = df_data.groupby(label_column)[['count']].sum().reset_index()
  df_total_count = df_total_count.rename(columns={'count': 'total_count'})

  # Joining total counts and calculating proportions.
  df_value_proportions = df_value_count.merge(df_total_count, on=label_column)
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

  # Plot distribution of the feature values by label
  viz_utils.plot_bar(
      plot_data=df_value_proportions,
      x_variable='value',
      y_variable='percentage',
      group_variable=label_column,
      title=f'Distribution of [{feature_name}]',
      subplot_index=0,
      **common_barplot_params)

  # Plot the snapshot-level distribution of the feature for positive instances
  pos_instance_stats = df_data[df_data[label_column] == positive_class_label]
  pos_instance_stats = pos_instance_stats.sort_values(
      ['snapshot_date', 'feature'], ascending=True)

  pos_plot_title = (f'Snapshot-level distribution of [{feature_name}] for '
                    f'label = {positive_class_label}')
  viz_utils.plot_bar(
      plot_data=pos_instance_stats,
      x_variable='snapshot_date',
      y_variable='percentage',
      group_variable='value',
      stacked_bars=True,
      title=pos_plot_title,
      subplot_index=1,
      xticklabels_rotation=45,
      x_label='',  # to better arrange the output plots
      **common_barplot_params)

  # Plot the snapshot-level distribution of the feature for negative instances
  neg_instance_stats = df_data[df_data[label_column] == negative_class_label]
  neg_instance_stats = neg_instance_stats.sort_values(
      ['snapshot_date', 'feature'], ascending=True)

  neg_plot_title = (f'Snapshot-level distribution of [{feature_name}] for '
                    f'label = {negative_class_label}')
  viz_utils.plot_bar(
      plot_data=neg_instance_stats,
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


def _plot_categorical_feature_numerical_label(
    label_stats_data: pd.DataFrame, feature_stats_data: pd.DataFrame,
    feature_name: str,
    plot_style_params: _FeaturePlotStyles) -> List[axes.Axes]:
  """Plots the statistics of a categorical feature when label is numerical.

  Generates following plots:
  - distribution of the label (box plots) for different category values of the
      feature.
  - distribution of feature values (stacked bor plots) by snapshot_date.

  Args:
    label_stats_data: Plot data containing the following columns: feature,
      value, mean, stddev, med, q1, q3, whislo and whishi.
    feature_stats_data: Plot data containing the following columns:
      snapshot_date, record_count, prop_missing, prop_non_num, average and
      stddev.
    feature_name: Name of the feature.
    plot_style_params: Plot style parameters.

  Returns:
     plots: A list of Axes containing 2 plots.
  """
  logging.info('Plotting categorical feature %s', feature_name)

  _, plots = pyplot.subplots(
      nrows=_NO_PLOTS_CAT_FEATURE_NUM_LABEL,
      ncols=_COLS_IN_SUBPLOTS_GRID,
      figsize=(plot_style_params.fig_width, plot_style_params.fig_height))

  # Plot distribution of the label by different feature values (categories)
  logging.info('Plotting label distribution by feature category values.')
  viz_utils.plot_box(
      plot_data=label_stats_data,
      title=f'Label distribution by [{feature_name}] categories',
      axes=plots,
      subplot_index=0,
      x_variable='value',
      x_label='Category value',
      y_label='Label distribution',
      title_fontsize=plot_style_params.title_fontsize,
      xlabel_fontsize=plot_style_params.xlabel_fontsize,
      ylabel_fontsize=plot_style_params.ylabel_fontsize,
      xticklabels_fontsize=plot_style_params.xticklabels_fontsize,
      yticklabels_fontsize=plot_style_params.yticklabels_fontsize,
      xticklabels_rotation=0)

  # Plot snapshot-level feature distribution
  viz_utils.plot_bar(
      plot_data=feature_stats_data,
      x_variable='snapshot_date',
      y_variable='percentage',
      group_variable='value',
      stacked_bars=True,
      title=f'Snapshot-level distribution of [{feature_name}]',
      subplot_index=1,
      axes=plots,
      title_fontsize=plot_style_params.title_fontsize,
      xlabel_fontsize=plot_style_params.xlabel_fontsize,
      ylabel_fontsize=plot_style_params.ylabel_fontsize,
      xticklabels_fontsize=plot_style_params.xticklabels_fontsize,
      yticklabels_fontsize=plot_style_params.yticklabels_fontsize,
      xticklabels_rotation=45,
  )

  return plots


class FeatureVisualizer(object):
  """This class provides methods to visualize the ML features.

  Features table is created by the GenerateFeaturesPipeline of
  MLDataWindowingPipeline.
  """

  def __init__(self,
               bq_client: bigquery.client.Client,
               features_table_path: str,
               numerical_features: Sequence[str],
               categorical_features: Sequence[str],
               label_column: str,
               label_type: str,
               positive_class_label: Optional[LabelType] = None,
               negative_class_label: Optional[LabelType] = None,
               num_instances: Optional[int] = 1000,
               num_pos_instances: Optional[int] = 1000,
               num_neg_instances: Optional[int] = 1000) -> None:
    """Initialises parameters.

    Args:
      bq_client: Connection object to the Bigquery account.
      features_table_path: Full path to the BigQuery Features table. example:
        'project_id.dataset.features_table'.
      numerical_features: List of numerical feature names to calculate
        statistics for.
      categorical_features: List of categorical feature names to calculate
        statistics for.
      label_column: Name of the label column of the Instance table.
      label_type: Type of the label column. Should be either 'binary' or
        'numerical'.
      positive_class_label: Label value representing the positive class
        instances. Should contain a value when the label_type is 'binary'.
      negative_class_label: Label value representing the negative class
        instances. Should contain a value when the label_type is 'binary'.
      num_instances: Number of instances to randomly select for numerical
        feature visualization. Active when the label_type is 'numerical'.
      num_pos_instances: Number of positive instances to randomly select for
        numerical feature visualization. Active when the label_type is 'binary'.
      num_neg_instances: Number of negative instances to randomly select for
        numerical feature visualization. Active when the label_type is 'binary'.
    """
    if label_type not in ['binary', 'numerical']:
      raise ValueError("label_type should contain either 'binary' or"
                       "'numerical' as the value")

    if ((label_type == 'binary') and
        ((positive_class_label is None) or (negative_class_label is None))):
      raise ValueError('When the label_type is binary, positive_class_label '
                       'and negative_class_label should contain values other '
                       'than None')

    self._bq_client = bq_client
    self._features_table_path = features_table_path
    self._numerical_feature_list = list(numerical_features)
    self._categorical_feature_list = list(categorical_features)
    self._label_column = label_column
    self._label_type = label_type
    self._positive_class_label = positive_class_label
    self._negative_class_label = negative_class_label
    self._num_instances = num_instances
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

    sql_template_path = ''
    if self._label_type == 'binary':
      sql_template_path = _BINARY_LABEL_SQL_FILES['calc_num_feature_stats']
    else:
      sql_template_path = _NUMERICAL_LABEL_SQL_FILES['calc_num_feature_stats']

    sql_query = viz_utils.patch_sql(sql_template_path, query_params)
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
        'column_list_sql': sql_segment
    }

    sql_template_path = ''
    if self._label_type == 'binary':
      sql_template_path = _BINARY_LABEL_SQL_FILES['extract_num_feature']

      sql_positive_class_label = self._positive_class_label
      sql_negative_class_label = self._negative_class_label
      if isinstance(self._positive_class_label, str):
        sql_positive_class_label = f"'{self._positive_class_label}'"
      if isinstance(self._negative_class_label, str):
        sql_negative_class_label = f"'{self._negative_class_label}'"

      query_params.update({
          'positive_class_label': sql_positive_class_label,
          'negative_class_label': sql_negative_class_label,
          'num_pos_instances': self._num_pos_instances,
          'num_neg_instances': self._num_neg_instances
      })
    else:
      sql_template_path = _NUMERICAL_LABEL_SQL_FILES['extract_num_feature']
      query_params.update({'num_instances': self._num_instances})

    sql_query = viz_utils.patch_sql(sql_template_path, query_params)
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

    sql_template_path = ''
    if self._label_type == 'binary':
      sql_template_path = _BINARY_LABEL_SQL_FILES['calc_cat_feature_stats']
    else:
      sql_template_path = _NUMERICAL_LABEL_SQL_FILES['calc_cat_feature_stats']

    sql_query = viz_utils.patch_sql(sql_template_path, query_params)
    logging.info('Finished creating the sql code.')

    logging.info('Executing the sql code.')
    results = viz_utils.execute_sql(self._bq_client, sql_query)
    logging.info('Finished executing the sql code.')

    return results

  def _calc_label_stats_cat_feature(self) -> pd.DataFrame:
    """Calculates the statistics for label by categorical feature values.

    Returns:
      results: Calculated statistics.
    """
    logging.info('Calculating statistics from label.')
    logging.info('Creating the sql code.')
    sql_segment = self._create_struct_column_list_sql(
        self._categorical_feature_list)
    query_params = {
        'bq_features_table': self._features_table_path,
        'label_column': self._label_column,
        'sql_code_segment': sql_segment
    }

    sql_query = viz_utils.patch_sql(
        _NUMERICAL_LABEL_SQL_FILES['calc_num_label_stats'], query_params)
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
    if self._label_type == 'numerical':
      label_stats_cat_feature = self._calc_label_stats_cat_feature()

    all_plots = []

    logging.info('Plotting numerical features.')
    for feature_name in self._numerical_feature_list:
      num_plot_data = numerical_feature_stats[numerical_feature_stats['feature']
                                              == feature_name]
      cols = [feature_name, self._label_column]
      num_plot_data_sample = numerical_feature_sample[cols]
      if self._label_type == 'binary':
        all_plots.append(
            _plot_numerical_feature_binary_label(
                num_plot_data, num_plot_data_sample, feature_name,
                self._label_column, self._positive_class_label,
                self._negative_class_label, plot_style_params))
      else:
        all_plots.append(
            _plot_numerical_feature_numerical_label(num_plot_data,
                                                    num_plot_data_sample,
                                                    feature_name,
                                                    self._label_column,
                                                    plot_style_params))

    logging.info('Plotting categorical features.')
    for feature_name in self._categorical_feature_list:
      cat_plot_data = categorical_feature_stats[
          categorical_feature_stats['feature'] == feature_name]
      if self._label_type == 'binary':
        all_plots.append(
            _plot_categorical_feature_binary_label(cat_plot_data, feature_name,
                                                   self._label_column,
                                                   self._positive_class_label,
                                                   self._negative_class_label,
                                                   plot_style_params))
      else:
        label_stats_data = label_stats_cat_feature[
            label_stats_cat_feature['feature'] == feature_name]
        all_plots.append(
            _plot_categorical_feature_numerical_label(label_stats_data,
                                                      cat_plot_data,
                                                      feature_name,
                                                      plot_style_params))

    return all_plots
