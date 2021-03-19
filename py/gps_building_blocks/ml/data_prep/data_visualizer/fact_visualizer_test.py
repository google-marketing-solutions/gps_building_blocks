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

# Lint as: python3
"""Tests for google3.corp.gtech.ads.data_catalyst.components.data_preparation.data_visualizer.fact_visualizer."""

from absl.testing import absltest
from google.cloud import bigquery
import pandas as pd
from gps_building_blocks.ml import utils
from gps_building_blocks.ml.data_prep.data_visualizer import fact_visualizer

NUMERICAL_FACT_STATS = pd.DataFrame({
    'date': ['2019-10-01', '2019-10-01', '2019-10-02', '2019-10-02'],
    'fact_name': ['num_fact1', 'num_fact2', 'num_fact1', 'num_fact2'],
    'total_record_count': [1200, 750, 1000, 700],
    'average': [25, 10, 20, 12],
    'stddev': [2.5, 1.8, 2.4, 2.0],
})

CATEGORICAL_FACT_STATS = pd.DataFrame({
    'date': ['2019-10-01', '2019-10-01', '2019-10-01', '2019-10-01'],
    'fact_name': ['cat_fact1', 'cat_fact1', 'cat_fact2', 'cat_fact2'],
    'category_value': ['A', 'B', 'X', 'Y'],
    'record_count': [300, 700, 600, 900],
    'rank': [2, 1, 2, 1],
    'total_record_count': [1000, 1000, 1500, 1500],
    'percentage': [30.0, 70.0, 40.0, 60.0],
})


class FactVisualizerTest(absltest.TestCase):

  def setUp(self):
    self.addCleanup(absltest.mock.patch.stopall)
    super(FactVisualizerTest, self).setUp()

    self.mock_bq_client = absltest.mock.create_autospec(bigquery.client.Client)
    self.numerical_facts_table_path = 'project_id.dataset.num_facts_table'
    self.categorical_facts_table_path = 'project_id.dataset.cat_facts_table'
    self.number_top_categories = 5

    self.fact_viz_obj = fact_visualizer.FactVisualizer(
        bq_client=self.mock_bq_client,
        numerical_facts_table_path=self.numerical_facts_table_path,
        categorical_facts_table_path=self.categorical_facts_table_path,
        number_top_categories=self.number_top_categories)

    self.mock_configure_sql = absltest.mock.patch.object(
        utils, 'configure_sql', autospec=True).start()

  def test_plot_numerical_facts_returns_correct_plots(self):
    self.mock_bq_client.query.return_value.to_dataframe.return_value = NUMERICAL_FACT_STATS
    num_fact1_data = (
        NUMERICAL_FACT_STATS[NUMERICAL_FACT_STATS['fact_name'] == 'num_fact1'])
    num_fact1_total_record_count = list(num_fact1_data['total_record_count'])
    num_fact1_average = list(num_fact1_data['average'])
    num_fact1_stddev = list(num_fact1_data['stddev'])

    num_fact_plots = self.fact_viz_obj.plot_numerical_facts()
    num_fact_1_plots = num_fact_plots[0]

    with self.subTest(name='test the number of plots returned'):
      self.assertLen(num_fact_1_plots, 3)
    with self.subTest(
        name='test the elements of record counts plot of num_fact_1'):
      self.assertListEqual(
          num_fact1_total_record_count,
          list(num_fact_1_plots[0].get_lines()[0].get_data()[1]))
    with self.subTest(name='test the elements of average plot of num_fact_1'):
      self.assertListEqual(
          num_fact1_average,
          list(num_fact_1_plots[1].get_lines()[0].get_data()[1]))
    with self.subTest(name='test the elements of stdev plot of num_fact_1'):
      self.assertListEqual(
          num_fact1_stddev,
          list(num_fact_1_plots[2].get_lines()[0].get_data()[1]))

  def test_plot_categorical_facts_returns_correct_plots(self):
    self.mock_bq_client.query.return_value.to_dataframe.return_value = CATEGORICAL_FACT_STATS
    cat_fact1_data = (
        CATEGORICAL_FACT_STATS[CATEGORICAL_FACT_STATS['fact_name'] ==
                               'cat_fact1'])
    cat_fact1_tot_record_count = list(set(cat_fact1_data['total_record_count']))
    cat_fact1_category_values = sorted(
        list(set(cat_fact1_data['category_value'])))
    cat_fact1_category_percentage = list(set(cat_fact1_data['percentage']))

    cat_fact_plots = self.fact_viz_obj.plot_categorical_facts()
    cat_fact_1_plots = cat_fact_plots[0]

    with self.subTest(
        name='test the elements of record counts plot of cat_fact_1'):
      self.assertListEqual(
          cat_fact1_tot_record_count,
          list(cat_fact_1_plots[0].get_lines()[0].get_data()[1]))
    with self.subTest(
        name='test the elements of percentage plot of cat_fact_1'):
      self.assertListEqual(
          cat_fact1_category_values,
          sorted([
              tick.get_text() for tick in cat_fact_1_plots[1].get_xticklabels()
          ]))
      self.assertListEqual(
          cat_fact1_category_percentage,
          [h.get_height() for h in cat_fact_1_plots[1].patches])
    with self.subTest(name='test title of the percentage plot of cat_fact_1'):
      self.assertEqual('cat_fact1 - Daily value distribution (%)',
                       cat_fact_1_plots[2].get_title())


if __name__ == '__main__':
  absltest.main()
