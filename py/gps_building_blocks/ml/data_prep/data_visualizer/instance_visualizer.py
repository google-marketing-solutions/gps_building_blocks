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
"""Visualizes the Instances table created by the ML Windowing Pipeline module.

Calculates statistics from the Instance table in BigQuery, generates and outputs
plots.

Instance table is created by the DataExplorationPipeline of the
ML Windowing Pipeline tool. For more info:
https://github.com/google/gps_building_blocks/tree/master/py/gps_building_blocks/ml/data_prep/ml_windowing_pipeline
"""

from typing import Optional, Union
from absl import logging
from google.cloud import bigquery
import matplotlib
from matplotlib import pyplot
import pandas as pd
from gps_building_blocks.ml import utils
from gps_building_blocks.ml.data_prep.data_visualizer import viz_utils

_ROWS_IN_SUBPLOTS_GRID = 5
_COLS_IN_SUBPLOT_GRID = 1
# Path to the file with sql code to calculate stats from the Instance table in
# BigQuery.
_CALC_INSTANCE_STATS_SQL_PATH = viz_utils.get_absolute_path(
    'calc_instance_stats.sql')
# Path to the file with sql code to extract features from the Instance table in
# BigQuery.
_EXTRACT_INSTANCE_FEATURES_SQL_PATH = viz_utils.get_absolute_path(
    'extract_instance_features.sql')
# Type of the label values
LabelType = Union[str, bool, int]


class InstanceVisualizer(object):
  """This class provides methods to visualize statistics from Instance table.

  Instance table is created by the DataExplorationPipeline of
  MLDataWindowingPipeline, which contains the instances selected for modeling,
  labels and two features namely, daysSinceFirstActivity and
  daysSinceLatestActivity.
  """

  def __init__(self, bq_client: bigquery.client.Client,
               instance_table_path: str, num_instances: int, label_column: str,
               positive_class_label: LabelType,
               negative_class_label: LabelType) -> None:
    """Initializes parameters.

    Args:
      bq_client: Connection object to the Bigquery account.
      instance_table_path: Full path to the BigQuery Instance table. Example:
        'project_id.dataset.facts_table'.
      num_instances: Number of instances to select randomly for plotting their
        features.
      label_column: Name of the label column of the Instance table.
      positive_class_label: Label value representing the positive class
        instances.
      negative_class_label: Label value representing the negative class
        instances.
    """
    # Initialize class variables.
    self._bq_client = bq_client
    self._num_instances = num_instances
    self._instance_table_path = instance_table_path
    self._label_column = label_column
    self._positive_class_label = positive_class_label
    self._negative_class_label = negative_class_label

  def _calculate_instance_statistics(self) -> pd.DataFrame:
    """Calculates statistics from the Instance table in BigQuery.

    Returns:
      results: Calculated statistics as a DataFrame.
    """
    logging.info('Calculating statistics from Instance table.')
    logging.info('Reading the sql query from the file.')
    positive_class_label_sql = self._positive_class_label
    if (self._positive_class_label) == 'str':
      positive_class_label_sql = """'{self._positive_class_label}'"""
    negaive_class_label_sql = self._negative_class_label
    if isinstance(self._negative_class_label, str):
      negaive_class_label_sql = """'{self._negative_class_label}'"""
    query_params = {
        'bq_instance_table': self._instance_table_path,
        'label_column': self._label_column,
        'positive_class_label': positive_class_label_sql,
        'negative_class_label': negaive_class_label_sql
    }
    sql_query = utils.configure_sql(_CALC_INSTANCE_STATS_SQL_PATH, query_params)
    results = viz_utils.execute_sql(self._bq_client, sql_query)
    logging.info('Finished calculating statistics from Instance table.')
    return results

  def _extract_instance_features(self) -> pd.DataFrame:
    """Extracts features from Instance table in BigQuery.

      Here features mean pre-calculated information about ML data instances
      such as daysSinceFirstActivity and daysSinceLatestActivity.

    Returns:
      results: Extracted features as a DataFrame.
    """
    logging.info('Extracting features from Instance table.')
    logging.info('Reading the sql query from the file.')
    query_params = {
        'bq_instance_table': self._instance_table_path,
        'num_instances': self._num_instances,
        'label_column': self._label_column
    }
    sql_query = utils.configure_sql(_EXTRACT_INSTANCE_FEATURES_SQL_PATH,
                                    query_params)
    results = viz_utils.execute_sql(self._bq_client, sql_query)
    logging.info('Finished extracting features from Instance table.')
    return results

  def plot_instances(
      self,
      fig_width: Optional[int] = 10,
      fig_height: Optional[int] = 50,
      barplot_color: Optional[str] = 'blue',
      barplot_xlabel_fontsize: Optional[int] = 10,
      barplot_ylabel_fontsize: Optional[int] = 10,
      barplot_xticklabels_fontsize: Optional[int] = 10,
      barplot_yticklabels_fontsize: Optional[int] = 10,
      barplot_title_fontsize: Optional[int] = 15,
      density_color_positive_class: Optional[str] = 'green',
      density_color_negative_class: Optional[str] = 'blue',
      densityplot_ticklabels_fontsize: Optional[int] = 10,
      densityplot_title_fontsize: Optional[int] = 15,
      densityplot_legend_fontsize: Optional[int] = 10) -> matplotlib.axes.Axes:
    """Generates statistics from the instance table and executes plotting.

    Args:
      fig_width: Width of the figure.
      fig_height: Height of the figure.
      barplot_color: Color of the bar plots,
      barplot_xlabel_fontsize: X-axis label font size of the bar plots.
      barplot_ylabel_fontsize: Y-axis label font size of the bar plots.
      barplot_xticklabels_fontsize: X-axis tick label font size of the bar
        plots.
      barplot_yticklabels_fontsize: Y-axis tick label font size of the bar
        plots.
      barplot_title_fontsize: Title font size of the bar plots.
      density_color_positive_class: Density plot clolor of the positive class.
      density_color_negative_class: Density plot clolor of the negative class.
      densityplot_ticklabels_fontsize: Tick label font size of the density
        plots.
      densityplot_title_fontsize: Title font size of the density plots.
      densityplot_legend_fontsize: Legend font size of the density plots.

    Returns:
      plots: plots of the statistics generated from the instance table.
    """
    # Calculates statistics (data for ploting) from the Instance table.
    instance_statistics_data = self._calculate_instance_statistics()
    instance_features_data = self._extract_instance_features()

    # Executes plotting.
    _, plots = pyplot.subplots(
        nrows=_ROWS_IN_SUBPLOTS_GRID,
        ncols=_COLS_IN_SUBPLOT_GRID,
        figsize=(fig_width, fig_height))

    bar_plot_common_params = {
        'plot_data': instance_statistics_data,
        'x_variable': 'snapshot_date',
        'axes': plots,
        'x_label': 'Snapshot date',
        'bar_color': barplot_color,
        'title_fontsize': barplot_title_fontsize,
        'xlabel_fontsize': barplot_xlabel_fontsize,
        'ylabel_fontsize': barplot_ylabel_fontsize,
        'xticklabels_fontsize': barplot_xticklabels_fontsize,
        'yticklabels_fontsize': barplot_yticklabels_fontsize,
        'xticklabels_rotation': 45
    }

    logging.info('Plotting % of positive label distribution over time.')
    viz_utils.plot_bar(
        y_variable='positive_percentage',
        title='% of positive label distribution over time',
        subplot_index=0,
        y_label='Percentage',
        **bar_plot_common_params)

    logging.info('Plotting number of positive instances over time.')
    viz_utils.plot_bar(
        y_variable='pos_count',
        title='Number of positive instances over time',
        subplot_index=1,
        y_label='Count',
        **bar_plot_common_params)

    logging.info('Plotting number of total instances over time.')
    viz_utils.plot_bar(
        y_variable='tot_count',
        title='Number of total instances over time',
        subplot_index=2,
        y_label='Count',
        **bar_plot_common_params)

    density_plot_common_params = {
        'plot_data': instance_features_data,
        'label_variable': self._label_column,
        'class1_label': self._positive_class_label,
        'class2_label': self._negative_class_label,
        'axes': plots,
        'class1_color': density_color_positive_class,
        'class2_color': density_color_negative_class,
        'title_fontsize': densityplot_title_fontsize,
        'ticklabels_fontsize': densityplot_ticklabels_fontsize,
        'legend_fontsize': densityplot_legend_fontsize
    }

    logging.info('Plotting distribution of days_since_first_activity.')
    viz_utils.plot_class_densities(
        plot_variable='days_since_first_activity',
        title='Class distribution of days_since_first_activity',
        subplot_index=3,
        **density_plot_common_params)

    logging.info('Plotting distribution of days_since_latest_activity.')
    viz_utils.plot_class_densities(
        plot_variable='days_since_latest_activity',
        title='Class distribution of days_since_latest_activity',
        subplot_index=4,
        **density_plot_common_params)

    return plots
