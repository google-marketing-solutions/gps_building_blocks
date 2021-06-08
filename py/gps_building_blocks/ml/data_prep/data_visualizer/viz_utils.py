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
"""Contains functions to support data visualizations."""

import logging
from typing import Dict, Optional, Union
from google.cloud import bigquery
import importlib_resources
import matplotlib
from matplotlib import pyplot
import pandas as pd
from gps_building_blocks.ml import utils
from gps_building_blocks.ml.data_prep.data_visualizer import templates


def patch_sql(sql_path: str, query_params: Dict[str, Union[str, int,
                                                           float]]) -> str:
  """Patch an SQL script with query parameters or code segments.

  Args:
    sql_path: Path to SQL script.
    query_params: Configuration containing query parameter values or sql code
      segments.

  Returns:
    sql_script: String representation of the patched SQL script.
  """
  sql_script = utils.read_file(sql_path)
  return sql_script.format(**query_params)


def execute_sql(bq_client: bigquery.Client, sql_query: str) -> pd.DataFrame:
  """Executes an sql query synchronously.

  Args:
    bq_client: Connection object to the Bigquery account.
    sql_query: Sql query string to be executed.

  Returns:
    Results from the query.
  """
  logging.info('Started running the query.')
  query_job = bq_client.query(sql_query)
  # Wait for job to finish
  query_job.result()
  logging.info('Finished running the query.')

  return query_job.to_dataframe()


def plot_bar(plot_data: pd.DataFrame,
             x_variable: str,
             y_variable: str,
             title: str,
             axes: matplotlib.axes.Axes,
             subplot_index: int,
             x_label: Optional[str] = None,
             y_label: Optional[str] = None,
             group_variable: Optional[str] = None,
             stacked_bars: Optional[bool] = False,
             bar_color: Optional[str] = 'lightcoral',
             title_fontsize: Optional[int] = 15,
             xlabel_fontsize: Optional[int] = 10,
             ylabel_fontsize: Optional[int] = 10,
             xticklabels_fontsize: Optional[int] = 10,
             yticklabels_fontsize: Optional[int] = 10,
             xticklabels_rotation: Optional[int] = 0) -> None:
  """Generates a bar plot and attaches to the axes object.

  Args:
    plot_data: Data to plot conaining the columns x_variable and y_variable.
    x_variable: Variable name for X-axis.
    y_variable: Variable name for Y-axis.
    title: Plot title.
    axes: Axes arranging multiple subplots in a column which contains this plot.
    subplot_index: Position (index) of this plot in a column of subplots.
    x_label: Label for X-axis.
    y_label: Label for Y-axis.
    group_variable: Variable containing the group values when plotting a
      a bar plot per group on the same chart.
    stacked_bars: Flag indicating to stack the bars or not when plotting
      a bar plot per group on the same chart.
    bar_color: Color of bars when plotting a bar plot for one category.
    title_fontsize: Font size of plot title.
    xlabel_fontsize: Font size of X-axis label.
    ylabel_fontsize: Font size of Y-axis label.
    xticklabels_fontsize: Font size of X-axis tick labels.
    yticklabels_fontsize: Font size of Y-axis tick labels.
    xticklabels_rotation: Degrees of rotation for X-axis tick labels.
  """
  if group_variable is not None:
    plot_data_pivoted = plot_data.pivot(
        index=x_variable, columns=group_variable, values=y_variable)
    bar_plot = plot_data_pivoted.plot.bar(ax=axes[subplot_index],
                                          stacked=stacked_bars,
                                          rot=xticklabels_rotation)
  else:
    bar_plot = plot_data.plot.bar(
        x=x_variable,
        y=y_variable,
        ax=axes[subplot_index],
        color=str(bar_color).lower(),
        legend=False,
        rot=xticklabels_rotation)

  if x_label is None:
    x_label = x_variable
  if y_label is None:
    y_label = y_variable
  bar_plot.set_title(title, fontsize=title_fontsize)
  bar_plot.set_xlabel(x_label, fontsize=xlabel_fontsize)
  bar_plot.set_ylabel(y_label, fontsize=ylabel_fontsize)
  bar_plot.tick_params(axis='x', which='both', labelsize=xticklabels_fontsize)
  bar_plot.tick_params(axis='y', which='both', labelsize=yticklabels_fontsize)
  bar_plot.yaxis.grid(True, linestyle='dashed')
  bar_plot.set_axisbelow(True)


def plot_class_densities(plot_data: pd.DataFrame,
                         plot_variable: str,
                         class1_label: Union[str, bool, int],
                         class2_label: Union[str, bool, int],
                         label_variable: str,
                         title: str,
                         axes: matplotlib.axes.Axes,
                         subplot_index: int,
                         class1_color: Optional[str] = 'limegreen',
                         class2_color: Optional[str] = 'cornflowerblue',
                         title_fontsize: Optional[int] = 15,
                         ticklabels_fontsize: Optional[int] = 10,
                         legend_fontsize: Optional[int] = 10) -> None:
  """Plots class conditional distribution of a variable for 2 classes.

  Plots density plots of plot_variable for two claases (groups) defined in
  label_variable on the same plot and attaches to axes object.

  Args:
    plot_data: Data to plot containing the plot_variable and label_variable
      columns.
    plot_variable: Variable to plot.
    class1_label: Label string representing class (group) 1 in label_variable.
    class2_label: Label string representing class (group) 2 in label_variable.
    label_variable: Variable name of the label.
    title: Plot title.
    axes: Axes arranging multiple subplots in a column which contains this plot.
    subplot_index: Position (index) of this plot in a column of subplots.
    class1_color: Color of density plot 1.
    class2_color: Color of density plot 2.
    title_fontsize: Font size of plot title.
    ticklabels_fontsize: Font size of tick labels.
    legend_fontsize: Font size of the legend.
  """
  class1_data = pd.DataFrame(
      plot_data[plot_data[label_variable] == class1_label][plot_variable])
  class1_data.columns = [label_variable + '=' + str(class1_label)]
  density_plots = class1_data.plot.kde(
      legend=True, ax=axes[subplot_index], color=str(class1_color).lower())

  class2_data = pd.DataFrame(
      plot_data[plot_data[label_variable] == class2_label][plot_variable])
  class2_data.columns = [label_variable + '=' + str(class2_label)]
  class2_data.plot.kde(
      legend=True, ax=axes[subplot_index], color=str(class2_color).lower())

  density_plots.tick_params(labelsize=ticklabels_fontsize)
  density_plots.set_title(title, fontsize=title_fontsize)
  pyplot.setp(density_plots.get_legend().get_texts(), fontsize=legend_fontsize)


def plot_line(plot_data: pd.DataFrame,
              x_variable: str,
              y_variable: str,
              title: str,
              axes: matplotlib.axes.Axes,
              subplot_index: int,
              x_label: Optional[str] = None,
              y_label: Optional[str] = None,
              line_color: Optional[str] = 'cornflowerblue',
              category_variable: Optional[str] = None,
              title_fontsize: Optional[int] = 15,
              xlabel_fontsize: Optional[int] = 10,
              ylabel_fontsize: Optional[int] = 12,
              xticklabels_fontsize: Optional[int] = 10,
              yticklabels_fontsize: Optional[int] = 12,
              legend_fontsize: Optional[int] = 10,
              xticklabels_rotation: Optional[int] = 0) -> None:
  """Generates a line plot attaches to the axes object.

  Args:
    plot_data: Data to plot containing x_variable, y_variable and optionally
      hue_variable columns.
    x_variable: Variable name for X-axis.
    y_variable: Variable name for Y-axis.
    title: Plot title.
    axes: Axes arranging multiple subplots in a column which contains this plot.
    subplot_index: Position (index) of this plot in a column of subplots.
    x_label: Label for X-axis.
    y_label: Label for Y-axis.
    line_color: Color of the line when plotting an one-line plot.
    category_variable: Variable contains the category values when plotting a
      multi-line plot (a line per category).
    title_fontsize: Font size of plot title.
    xlabel_fontsize: Font size of X-axis label.
    ylabel_fontsize: Font size of Y-axis label.
    xticklabels_fontsize: Font size of x tick labels.
    yticklabels_fontsize: Font size of y tick labels.
    legend_fontsize: Font size of the legend.
    xticklabels_rotation: Degrees of rotation for X-axis tick labels.
  """
  if category_variable is not None:
    plot_data_pivoted = plot_data.pivot(
        index=x_variable, columns=category_variable, values=y_variable)
    line_plot = plot_data_pivoted.plot.line(ax=axes[subplot_index],
                                            rot=xticklabels_rotation)
    pyplot.setp(
        axes[subplot_index].get_legend().get_texts(), fontsize=legend_fontsize)
  else:
    line_plot = plot_data.plot.line(
        x=x_variable,
        y=y_variable,
        color=str(line_color).lower(),
        ax=axes[subplot_index],
        rot=xticklabels_rotation)

  if x_label is None:
    x_label = x_variable
  if y_label is None:
    y_label = y_variable
  line_plot.set_xlabel(x_label, fontsize=xlabel_fontsize)
  line_plot.set_ylabel(y_label, fontsize=ylabel_fontsize)
  line_plot.set_title(title, fontsize=title_fontsize)
  line_plot.tick_params(axis='x', labelsize=xticklabels_fontsize)
  line_plot.tick_params(axis='y', labelsize=yticklabels_fontsize)
  line_plot.yaxis.grid(True, linestyle='dashed')
  line_plot.set_axisbelow(True)


def get_absolute_path(file_name: str) -> str:
  """Retruns the absolute path of the input file name in the template directory.

  Args:
    file_name: File name in template directory of which the absolute path needed

  Returns:
    Absolute full path of the file name.
  """
  with importlib_resources.path(templates, file_name) as absolute_path:
    return str(absolute_path.absolute())
