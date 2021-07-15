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
"""Tests for google3.corp.gtech.ads.data_catalyst.components.data_preparation.feature_visualizer.instance_visualizer."""

from absl.testing import absltest
from google.cloud import bigquery
import pandas as pd
from gps_building_blocks.ml import utils
from gps_building_blocks.ml.data_prep.data_visualizer import instance_visualizer

BINARY_LABEL_INSTANCE_STATISTICS_DATA = pd.DataFrame({
    'snapshot_date': ['2019-10-01', '2019-10-02', '2019-10-03'],
    'pos_count': [326, 312, 300],
    'neg_count': [321061, 320396, 320200],
    'tot_count': [321387, 320708, 320500],
    'positive_percentage': [0.001014, 0.000973, 0.0009]
})

NUMERICAL_LABEL_INSTANCE_STATISTICS_DATA = pd.DataFrame({
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

BINARY_LABEL_INSTANCE_FEATURES_DATA = pd.DataFrame({
    'days_since_first_activity': [120, 150, 200, 60, 100, 130, 20, 50, 450, 38],
    'days_since_latest_activity': [30, 100, 35, 23, 45, 100, 14, 7, 20, 6],
    'label': [
        'True', 'False', 'False', 'True', 'False', 'True', 'False', 'False',
        'True', 'True'
    ]
})

NUMERICAL_LABEL_INSTANCE_FEATURES_DATA = pd.DataFrame({
    'days_since_first_activity': [120, 150, 200, 60, 100, 130, 20, 50, 450, 38],
    'days_since_latest_activity': [30, 100, 35, 23, 45, 100, 14, 7, 20, 6]
})


class InstanceVisualizerTest(absltest.TestCase):

  def setUp(self):
    self.addCleanup(absltest.mock.patch.stopall)
    super(InstanceVisualizerTest, self).setUp()

    self.mock_bq_client = absltest.mock.create_autospec(bigquery.client.Client)
    self.instance_table_path = 'project_id.dataset.instance_table'
    self.num_instances = 100000

  def test_plot_instances_returns_correct_plots_for_binary_label(self):
    self.ins_viz_obj = instance_visualizer.InstanceVisualizer(
        bq_client=self.mock_bq_client,
        instance_table_path=self.instance_table_path,
        num_instances=self.num_instances,
        label_column='label',
        label_type='binary',
        positive_class_label='True',
        negative_class_label='False')

    self.mock_configure_sql = absltest.mock.patch.object(
        utils, 'configure_sql', autospec=True).start()
    self.mock_bq_client.query.return_value.to_dataframe.side_effect = [
        BINARY_LABEL_INSTANCE_STATISTICS_DATA,
        BINARY_LABEL_INSTANCE_FEATURES_DATA
    ]

    x_data = list(BINARY_LABEL_INSTANCE_STATISTICS_DATA['snapshot_date'])
    y_data_positive_instances_rate = list(
        BINARY_LABEL_INSTANCE_STATISTICS_DATA['positive_percentage'])
    y_data_positive_instances_num = list(
        BINARY_LABEL_INSTANCE_STATISTICS_DATA['pos_count'])
    y_data_total_instances_num = list(
        BINARY_LABEL_INSTANCE_STATISTICS_DATA['tot_count'])

    feature_plot_1_title = 'Class distribution of days_since_first_activity'
    feature_plot_2_title = 'Class distribution of days_since_latest_activity'

    bar_plot_1, bar_plot_2, bar_plot_3, density_plot_1, density_plot_2 = (
        self.ins_viz_obj.plot_instances())

    with self.subTest(name='test the elements of total instances bar plot'):
      self.assertListEqual(
          x_data, [tick.get_text() for tick in bar_plot_1.get_xticklabels()])
      self.assertListEqual(y_data_total_instances_num,
                           [h.get_height() for h in bar_plot_1.patches])
    with self.subTest(name='test the elements of positive label rate bar plot'):
      self.assertListEqual(
          x_data, [tick.get_text() for tick in bar_plot_2.get_xticklabels()])
      self.assertListEqual(y_data_positive_instances_rate,
                           [h.get_height() for h in bar_plot_2.patches])
    with self.subTest(name='test the elements of positive instances bar plot'):
      self.assertListEqual(
          x_data, [tick.get_text() for tick in bar_plot_3.get_xticklabels()])
      self.assertListEqual(y_data_positive_instances_num,
                           [h.get_height() for h in bar_plot_3.patches])

    with self.subTest(name='test the title of the density plot 1'):
      self.assertEqual(feature_plot_1_title, density_plot_1.get_title())
    with self.subTest(name='test the title of the density plot 2'):
      self.assertEqual(feature_plot_2_title, density_plot_2.get_title())

  def test_plot_instances_returns_correct_plots_for_numerical_label(self):
    self.ins_viz_obj = instance_visualizer.InstanceVisualizer(
        bq_client=self.mock_bq_client,
        instance_table_path=self.instance_table_path,
        num_instances=self.num_instances,
        label_column='label',
        label_type='numerical',
        positive_class_label='True',
        negative_class_label='False')

    self.mock_configure_sql = absltest.mock.patch.object(
        utils, 'configure_sql', autospec=True).start()
    self.mock_bq_client.query.return_value.to_dataframe.side_effect = [
        NUMERICAL_LABEL_INSTANCE_STATISTICS_DATA,
        NUMERICAL_LABEL_INSTANCE_FEATURES_DATA
    ]

    x_data = list(NUMERICAL_LABEL_INSTANCE_STATISTICS_DATA['snapshot_date'])
    y_data_total_instances_num = list(
        NUMERICAL_LABEL_INSTANCE_STATISTICS_DATA['tot_count'])

    feature_plot_1_title = 'Distribution of days_since_first_activity'
    feature_plot_2_title = 'Distribution of days_since_latest_activity'

    bar_plot_1, box_plot_1, density_plot_1, density_plot_2 = (
        self.ins_viz_obj.plot_instances())

    with self.subTest(name='test the elements of total instances bar plot'):
      self.assertListEqual(
          x_data, [tick.get_text() for tick in bar_plot_1.get_xticklabels()])
      self.assertListEqual(y_data_total_instances_num,
                           [h.get_height() for h in bar_plot_1.patches])
    with self.subTest(name='test the elements of label distribution box plot'):
      self.assertListEqual(
          x_data, [tick.get_text() for tick in box_plot_1.get_xticklabels()])

    with self.subTest(name='test the title of the density plot 1'):
      self.assertEqual(feature_plot_1_title, density_plot_1.get_title())
    with self.subTest(name='test the title of the density plot 2'):
      self.assertEqual(feature_plot_2_title, density_plot_2.get_title())


if __name__ == '__main__':
  absltest.main()
