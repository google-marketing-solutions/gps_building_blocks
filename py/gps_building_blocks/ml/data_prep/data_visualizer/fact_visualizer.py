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
"""Visualizes the Facts table created by the MLDataWindowingPipeline.

Calculates statistics from the numerical and categoticals variables in the Facts
table in BigQuery and generates and outputs plots.These plots can be used to
explore the data to understand the distributions and any anomalies.

Facts table is created by the DataExplorationPipeline of the
MLDataWindowingPipeline tool. For more info:
https://cloud.google.com/solutions/preparing-ml-ready-data-for-personalization
"""

from typing import List, Optional, Sequence
from absl import logging
from google.cloud import bigquery
from matplotlib import axes
from matplotlib import pyplot
import pandas as pd
from gps_building_blocks.ml import utils
from gps_building_blocks.ml.data_prep.data_visualizer import viz_utils

# _plot_numerical_fact and _plot_categorical_fact functions in the below class
# FactVisualizer utilize these constants to generate 3 plots and arrange them
# in a single colum. By chaging the values of these constants will break the
# code.
_ROWS_IN_SUBPLOTS_GRID = 3
_COLS_IN_SUBPLOTS_GRID = 1
_CALC_NUM_FACT_STATS_SQL_PATH = 'templates/calc_numerical_fact_stats.sql'
_CALC_CAT_FACT_STATS_SQL_PATH = 'templates/calc_categorical_fact_stats.sql'


class _FactPlotStyles:
  """This class encapsulates variables to control the styles of fact plots."""

  def __init__(self,
               fig_width: Optional[int] = 10,
               fig_height: Optional[int] = 30,
               line_color_record_count: Optional[str] = 'blue',
               line_color_average: Optional[int] = 'coral',
               line_color_stddev: Optional[int] = 'lightcoral',
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
      line_color_record_count: Line color of the record count plot.
      line_color_average: Line color of the average plot.
      line_color_stddev: Line color of the standard deviation plot.
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
    self.line_color_record_count = line_color_record_count
    self.line_color_average = line_color_average
    self.line_color_stddev = line_color_stddev
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


def _plot_numerical_fact(plot_data: pd.DataFrame, fact_name: str,
                         plot_style_params: _FactPlotStyles) -> List[axes.Axes]:
  """Plots the statistics of a numerical fact variable.

  Generates plots of daily record count, daily average and daily standard
  deviation of a categorical fact variable.

  Args:
    plot_data: Data to plot containing date, record_count, average and stddev
      columns.
    fact_name: Name of the fact variable.
    plot_style_params: Plot style parameters.

  Returns:
     plots: A list of Axes containing 3 plots.
  """
  logging.info('Plotting numerical fact %s', fact_name)

  _, plots = pyplot.subplots(
      nrows=_ROWS_IN_SUBPLOTS_GRID,
      ncols=_COLS_IN_SUBPLOTS_GRID,
      figsize=(plot_style_params.fig_width, plot_style_params.fig_height))

  common_params = {
      'plot_data': plot_data,
      'x_variable': 'date',
      'axes': plots,
      'title_fontsize': plot_style_params.lineplot_title_fontsize,
      'xlabel_fontsize': plot_style_params.lineplot_xlabel_fontsize,
      'ylabel_fontsize': plot_style_params.lineplot_ylabel_fontsize,
      'xticklabels_fontsize': plot_style_params.lineplot_xticklabels_fontsize,
      'yticklabels_fontsize': plot_style_params.lineplot_yticklabels_fontsize
  }

  # plot daily total record count
  viz_utils.plot_line(
      y_variable='total_record_count',
      title=f'{fact_name}: Daily Record Count',
      subplot_index=0,
      line_color=plot_style_params.line_color_record_count,
      **common_params)

  # plot daily average
  viz_utils.plot_line(
      y_variable='average',
      title=f'{fact_name}: Daily Average per User',
      subplot_index=1,
      line_color=plot_style_params.line_color_average,
      **common_params)

  # plot daily standard deviation
  viz_utils.plot_line(
      y_variable='stddev',
      title=f'{fact_name}: Daily Standard Deviation per User',
      subplot_index=2,
      line_color=plot_style_params.line_color_stddev,
      **common_params)

  return plots


def _plot_categorical_fact(
    plot_data: pd.DataFrame, fact_name: str,
    plot_style_params: _FactPlotStyles) -> List[axes.Axes]:
  """Plots the statistics of a categorical fact variable.

  Generates plots of daily record count, latest distribution of top N levels
  and daily distribution of top N level over time of a categorical fact
  variable.

  Args:
    plot_data: Data to plot containing date, total_count, value and percentage
      columns.
    fact_name: Name of the fact variable.
    plot_style_params: Plot style parameters.

  Returns:
    plots: A list of Axes containing 3 plots.
  """
  logging.info('Plotting categorical fact %s ', fact_name)

  _, plots = pyplot.subplots(
      nrows=_ROWS_IN_SUBPLOTS_GRID,
      ncols=_COLS_IN_SUBPLOTS_GRID,
      figsize=(plot_style_params.fig_width, plot_style_params.fig_height))

  latest_date = max(plot_data['date'])
  latest_date_stats = plot_data[plot_data['date'] == latest_date].sort_values(
      by=['percentage'], ascending=False)

  common_lineplot_params = {
      'axes': plots,
      'title_fontsize': plot_style_params.lineplot_title_fontsize,
      'xticklabels_fontsize': plot_style_params.lineplot_xticklabels_fontsize,
      'yticklabels_fontsize': plot_style_params.lineplot_yticklabels_fontsize
  }

  # plot daily total fact count
  viz_utils.plot_line(
      plot_data=plot_data[['date', 'total_record_count']].drop_duplicates(),
      x_variable='date',
      y_variable='total_record_count',
      title=f'{fact_name} - Daily Fact Count',
      subplot_index=0,
      line_color=plot_style_params.line_color_record_count,
      **common_lineplot_params)

  # plot the latest distribution of the top N fact levels.
  viz_utils.plot_bar(
      plot_data=latest_date_stats,
      x_variable='category_value',
      y_variable='percentage',
      title=f'{fact_name} - Latest Value Distribution (%)',
      axes=plots,
      subplot_index=1,
      title_fontsize=plot_style_params.barplot_title_fontsize,
      xticklabels_fontsize=plot_style_params.barplot_xticklabels_fontsize,
      yticklabels_fontsize=plot_style_params.barplot_yticklabels_fontsize)

  # plot the daily distribution of the top N fact levels over time.
  viz_utils.plot_line(
      plot_data=plot_data,
      x_variable='date',
      y_variable='percentage',
      title=f'{fact_name} - Daily value distribution (%)',
      subplot_index=2,
      category_variable='category_value',
      legend_fontsize=plot_style_params.lineplot_legend_fontsize,
      **common_lineplot_params)

  return plots


class FactVisualizer(object):
  """This class provides methods to visualize the Facts table.

  Fact table is created by the DataVisualizationPipeline of
  MLDataWindowingPipeline. Facts table contains the original data transformed
  into (time, user, variable, value) format (called fact format, hence the name
  Facts table).
  """

  def __init__(self, bq_client: bigquery.client.Client, facts_table_path: str,
               numerical_facts: Sequence[str], categorical_facts: Sequence[str],
               number_top_levels: int) -> None:
    """Initialises parameters.

    Args:
      bq_client: Connection object to the Bigquery account.
      facts_table_path: Full path to the BigQuery facts table. example:
        'project_id.dataset.facts_table'.
      numerical_facts: Sequence of numerical fact names to calculate statistics
        for.
      categorical_facts: Sequence of categorical fact names to calculate
        statistics for.
      number_top_levels: Number of top value levels to consider for each
        categorical fact.
    """
    self._bq_client = bq_client
    self._facts_table_path = facts_table_path
    self._numerical_facts = numerical_facts
    self._categorical_facts = categorical_facts
    self._number_top_levels = number_top_levels

  def _calc_numerical_fact_stats(self) -> pd.DataFrame:
    """Calculates the statistics for selected numerical fact variables.

    Returns:
      results: Calculated statistics.
    """
    logging.info('Calculating statistics from numerical facts.')
    logging.info('Reading the sql query from the file.')
    query_params = {
        'bq_facts_table': self._facts_table_path,
        'numerical_fact_list': self._numerical_facts
    }
    sql_query = utils.configure_sql(_CALC_NUM_FACT_STATS_SQL_PATH, query_params)

    results = viz_utils.execute_sql(self._bq_client, sql_query)
    logging.info('Finished calculating statistics from numerical facts.')

    return results

  def _calc_categorical_fact_stats(self) -> pd.DataFrame:
    """Calculates the statistics for selected categorical fact variables.

    Returns:
      results: Calculated statistics.
    """
    logging.info('Calculating statistics from categorical facts.')
    logging.info('Reading the sql query from the file.')
    query_params = {
        'bq_facts_table': self._facts_table_path,
        'categorical_fact_list': self._categorical_facts,
        'number_top_levels': self._number_top_levels
    }
    sql_query = utils.configure_sql(_CALC_CAT_FACT_STATS_SQL_PATH, query_params)

    results = viz_utils.execute_sql(self._bq_client, sql_query)
    logging.info('Finished calculating statistics from categorical facts.')

    return results

  def plot_facts(
      self,
      fig_width: Optional[int] = 10,
      fig_height: Optional[int] = 30,
      line_color_record_count: Optional[int] = 'blue',
      line_color_average: Optional[int] = 'coral',
      line_color_stddev: Optional[int] = 'lightcoral',
      lineplot_title_fontsize: Optional[int] = 15,
      lineplot_legend_fontsize: Optional[int] = 10,
      lineplot_xlabel_fontsize: Optional[int] = 10,
      lineplot_ylabel_fontsize: Optional[int] = 10,
      lineplot_xticklabels_fontsize: Optional[int] = 10,
      lineplot_yticklabels_fontsize: Optional[int] = 10,
      barplot_title_fontsize: Optional[int] = 10,
      barplot_legend_fontsize: Optional[int] = 10,
      barplot_xlabel_fontsize: Optional[int] = 10,
      barplot_ylabel_fontsize: Optional[int] = 10,
      barplot_xticklabels_fontsize: Optional[int] = 10,
      barplot_yticklabels_fontsize: Optional[int] = 10
  ) -> List[List[axes.Axes]]:
    """Generates data and execute plotting.

    Args:
      fig_width: Width of the figure.
      fig_height: Height of the figure.
      line_color_record_count: Line color of the daily record count plot.
      line_color_average: Line color of the daily average value plot.
      line_color_stddev: Line color of the daily standard deviation value plot.
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
      all_plots: all the plots generated for the selected facts.
    """
    plot_style_params = _FactPlotStyles(
        fig_width=fig_width,
        fig_height=fig_height,
        line_color_record_count=line_color_record_count,
        line_color_average=line_color_average,
        line_color_stddev=line_color_stddev,
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

    numerical_fact_stats = self._calc_numerical_fact_stats()
    categorical_fact_stats = self._calc_categorical_fact_stats()

    all_plots = []

    logging.info('Plotting numerical facts.')
    for fact_name in self._numerical_facts:
      num_plot_data = numerical_fact_stats[numerical_fact_stats['fact_name'] ==
                                           fact_name]
      all_plots.append(
          _plot_numerical_fact(num_plot_data, fact_name, plot_style_params))

    logging.info('Plotting categorical facts.')
    for fact_name in self._categorical_facts:
      cat_plot_data = categorical_fact_stats[categorical_fact_stats['fact_name']
                                             == fact_name]
      all_plots.append(
          _plot_categorical_fact(cat_plot_data, fact_name, plot_style_params))

    return all_plots
