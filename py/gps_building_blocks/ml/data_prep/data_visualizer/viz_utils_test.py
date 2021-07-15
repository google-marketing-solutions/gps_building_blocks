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
"""Tests for google3.corp.gtech.ads.data_catalyst.components.data_preparation.data_visualizer.plot_data_creation_utils."""

from absl.testing import absltest
from google.cloud import bigquery
from matplotlib import pyplot as plt
import pandas as pd
from gps_building_blocks.ml.data_prep.data_visualizer import viz_utils

TESTDATA_1 = pd.DataFrame({
    'snapshot_date': ['2019-10-01', '2019-10-02', '2019-10-03'],
    'pos_count': [326, 312, 300],
    'neg_count': [321061, 320396, 320200],
    'tot_count': [321387, 320708, 320500],
    'rate': [0.001014, 0.000973, 0.0009]
})

TESTDATA_2 = pd.DataFrame({
    'days_since_first_activity': [120, 150, 200, 60, 100, 130, 20, 50, 450, 38],
    'days_since_latest_activity': [30, 100, 35, 23, 45, 100, 14, 7, 20, 6],
    'has_positive_label': [
        'True', 'False', 'False', 'True', 'False', 'True', 'False', 'False',
        'True', 'True'
    ]
})

TESTDATA_3 = pd.DataFrame({
    'snapshot_date': ['2019-10-01', '2019-10-02', '2019-10-03'],
    'tot_count': [321387, 320708, 320500],
    'mean': [101, 101, 102],
    'max': [150, 155, 170],
    'min': [50, 55, 57],
    'med': [98, 99, 99],
    'q1': [75, 75, 96],
    'q3': [120, 121, 122],
    'whislo': [60, 61, 62],
    'whishi': [130, 131, 132],
})

TESTDATA_4 = pd.DataFrame({
    'days_since_first_activity': [120, 150, 200, 60, 100, 130, 20, 50, 450, 38],
    'days_since_latest_activity': [30, 100, 35, 23, 45, 100, 14, 7, 20, 6]
})


class VizUtilsTest(absltest.TestCase):

  def setUp(self):
    self.addCleanup(absltest.mock.patch.stopall)
    super(VizUtilsTest, self).setUp()

    self.mock_bq_client = absltest.mock.create_autospec(bigquery.client.Client)

  def test_execute_sql_returns_pd_dataframe(self):
    fake_sql_query = 'SELECT * FROM project.dataset.table;'

    self.mock_bq_client.query.return_value.to_dataframe.return_value = TESTDATA_1

    results = viz_utils.execute_sql(self.mock_bq_client, fake_sql_query)

    self.mock_bq_client.query.return_value.result.assert_called_once()
    pd.testing.assert_frame_equal(results, TESTDATA_1)

  def test_plot_bar_returns_a_bar_plot_with_correct_elements(self):
    plot_data = TESTDATA_1
    x_var = 'snapshot_date'
    y_var = 'pos_count'
    title = '# positive examples over time'
    subplot_index = 0

    _, axes = plt.subplots(nrows=2, ncols=1)
    viz_utils.plot_bar(
        plot_data=plot_data,
        x_variable=x_var,
        y_variable=y_var,
        title=title,
        axes=axes,
        subplot_index=subplot_index)

    bar_plot = axes[subplot_index]
    x_data = list(plot_data[x_var])
    y_data = [float(y) for y in list(plot_data[y_var])]

    with self.subTest(name='test x axis variable is equal'):
      self.assertEqual(x_var, bar_plot.get_xlabel())
    with self.subTest(name='test x axis data is equal'):
      self.assertListEqual(
          x_data,
          [tick.get_text() for tick in bar_plot.get_xticklabels(which='major')])
    with self.subTest(name='test y axis variable is equal'):
      self.assertEqual(y_var, bar_plot.get_ylabel())
    with self.subTest(name='test y axis data is equal'):
      self.assertListEqual(y_data, [h.get_height() for h in bar_plot.patches])
    with self.subTest(name='test title is equal'):
      self.assertEqual(title, bar_plot.get_title())

  def test_plot_class_densities_returns_plot_with_correct_elements(self):
    plot_data = TESTDATA_2
    plot_variable = 'days_since_first_activity'
    title = 'Class distribution of days_since_first_activity'
    class1_label = 'True'
    class2_label = 'False'
    label_variable = 'has_positive_label'
    subplot_index = 0

    _, axes = plt.subplots(nrows=2, ncols=1)
    viz_utils.plot_class_densities(
        plot_data=plot_data,
        plot_variable=plot_variable,
        class1_label='True',
        class2_label='False',
        label_variable=label_variable,
        title=title,
        axes=axes,
        subplot_index=subplot_index)

    plot = axes[subplot_index]

    with self.subTest(name='test labels of two clases are equal'):
      self.assertListEqual([
          label_variable + '=' + class1_label,
          label_variable + '=' + class2_label
      ], [l.get_text() for l in plot.get_legend().get_texts()])
    with self.subTest(name='test title is equal'):
      self.assertEqual(title, plot.title.get_text())

  def test_plot_line_returns_a_line_plot_with_correct_elements(self):
    plot_data = TESTDATA_1
    x_var = 'snapshot_date'
    y_var = 'tot_count'
    title = 'Total examples over time'
    subplot_index = 0

    _, axes = plt.subplots(nrows=2, ncols=1)
    viz_utils.plot_line(
        plot_data=plot_data,
        x_variable=x_var,
        y_variable=y_var,
        title=title,
        axes=axes,
        subplot_index=subplot_index)

    line_plot = axes[subplot_index]
    y_data = list(plot_data[y_var])

    with self.subTest(name='test x axis variable is equal'):
      self.assertEqual(x_var, line_plot.get_xlabel())
    with self.subTest(name='test y axis variable is equal'):
      self.assertEqual(y_var, line_plot.get_ylabel())
    with self.subTest(name='test y axis data is equal'):
      self.assertListEqual(y_data, list(line_plot.get_lines()[0].get_data()[1]))
    with self.subTest(name='test title is equal'):
      self.assertEqual(title, line_plot.get_title())

  def test_plot_density_returns_plot_with_correct_elements(self):
    plot_data = TESTDATA_4
    plot_variable = 'days_since_first_activity'
    title = 'Distribution of days_since_first_activity'
    subplot_index = 0

    _, axes = plt.subplots(nrows=2, ncols=1)
    viz_utils.plot_density(
        plot_data=plot_data,
        plot_variable=plot_variable,
        title=title,
        axes=axes,
        subplot_index=subplot_index)

    plot = axes[subplot_index]

    with self.subTest(name='test title is equal'):
      self.assertEqual(title, plot.title.get_text())

  def test_plot_box_returns_plot_with_correct_elements(self):
    plot_data = TESTDATA_3
    x_variable = 'snapshot_date'
    title = 'Distribution of labels over time (approximated box plot)'
    subplot_index = 0
    lines_per_box = 7

    _, axes = plt.subplots(nrows=2, ncols=1)
    viz_utils.plot_box(
        plot_data=plot_data,
        x_variable=x_variable,
        title=title,
        axes=axes,
        subplot_index=subplot_index)

    plot = axes[subplot_index]

    with self.subTest(name='test title is equal'):
      self.assertEqual(title, plot.title.get_text())
    with self.subTest(name='test number of box lines is equal'):
      self.assertLen(plot.lines, len(TESTDATA_3) * lines_per_box)
    with self.subTest(name='test the elements of label distribution box plot'):
      self.assertListEqual(TESTDATA_3[x_variable].values.tolist(),
                           [tick.get_text() for tick in plot.get_xticklabels()])


if __name__ == '__main__':
  absltest.main()
