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
"""Tests for google3.corp.gtech.ads.data_catalyst.components.data_preparation.data_visualizer.feature_visualizer."""

from absl.testing import absltest
from google.cloud import bigquery
import pandas as pd
from gps_building_blocks.ml import utils
from gps_building_blocks.ml.data_prep.data_visualizer import feature_visualizer

BINARY_LABEL_NUMERICAL_FEATURES_SAMPLE = pd.DataFrame({
    'num_feature1': [8, 10, 5, 20, 2],
    'num_feature2': [100, 50, 25, 120, 23],
    'label': ['False', 'True', 'False', 'True', 'False']
})

NUMERICAL_LABEL_NUMERICAL_FEATURES_SAMPLE = pd.DataFrame({
    'num_feature1': [8, 10, 5, 20, 2],
    'num_feature2': [100, 50, 25, 120, 23],
    'label': [10, 20, 22, 30, 15]
})

BINARY_LABEL_NUMERICAL_FEATURES_STATS = pd.DataFrame({
    'snapshot_date': [
        '2015-01-05', '2015-01-05', '2015-01-06', '2015-01-06'
    ],
    'feature': [
        'num_feature1', 'num_feature1', 'num_feature1', 'num_feature1',
    ],
    'label': ['False', 'True', 'False', 'True'],
    'record_count': [56271, 123, 51632, 98],
    'prop_missing': [0.0, 0.0, 0.0, 0.0],
    'prop_non_num': [0.0, 0.0, 0.0, 0.0],
    'mean': [1.25, 6.02, 1.24, 5.20],
    'stddev': [0.93, 8.59, 0.90, 4.71],
    'med': [1.20, 5.7, 1.2, 5.00],
    'q1': [0.50, 0.31, 0.26, 1.2],
    'q3': [2.34, 6.32, 3.44, 7.54],
    'whislo': [0.02, 0.11, 0.08, 0.88],
    'whishi': [2.50, 6.40, 3.65, 7.90]
})

NUMERICAL_LABEL_NUMERICAL_FEATURES_STATS = pd.DataFrame({
    'snapshot_date': [
        '2015-01-05', '2015-01-06', '2015-01-05', '2015-01-06'
    ],
    'feature': [
        'num_feature1', 'num_feature1', 'num_feature2', 'num_feature2'
    ],
    'record_count': [400, 500, 450, 475],
    'prop_missing': [0.0, 0.01, 0.0, 0.0],
    'prop_non_num': [0.0, 0.0, 0.0, 0.0],
    'mean': [1.25, 6.02, 1.24, 5.20],
    'stddev': [0.93, 8.59, 0.90, 4.71],
    'med': [1.20, 5.7, 1.2, 5.00],
    'q1': [0.50, 0.31, 0.26, 1.2],
    'q3': [2.34, 6.32, 3.44, 7.54],
    'whislo': [0.02, 0.11, 0.08, 0.88],
    'whishi': [2.50, 6.40, 3.65, 7.90]
})

BINARY_LABEL_CATEGORICAL_FEATURES_STATS = pd.DataFrame({
    'snapshot_date': [
        '2017-01-05 00:00:00', '2017-01-05 00:00:00',
        '2017-01-05 00:00:00', '2017-01-05 00:00:00'
    ],
    'feature': [
        'cat_feature1', 'cat_feature1', 'cat_feature1', 'cat_feature1'
    ],
    'value': ['val1', 'val2', 'val1', 'val2'],
    'label': ['True', 'True', 'False', 'False'],
    'count': [113, 74, 15500, 12000],
    'total': [187, 187, 27500, 27500],
    'percentage': [
        60.4278, 39.5721, 56.3636, 43.6363]
})

NUMERICAL_LABEL_CATEGORICAL_FEATURES_STATS = pd.DataFrame({
    'snapshot_date': [
        '2017-01-05 00:00:00', '2017-01-05 00:00:00',
        '2017-01-06 00:00:00', '2017-01-06 00:00:00'
    ],
    'feature': [
        'cat_feature1', 'cat_feature1', 'cat_feature1', 'cat_feature1'
    ],
    'value': ['val1', 'val2', 'val1', 'val2'],
    'count': [113, 74, 157, 93],
    'total': [187, 187, 250, 250],
    'percentage': [
        60.4278, 39.5721, 62.8, 37.2]
})

NUMERICAL_LABEL_STATS = pd.DataFrame({
    'feature': [
        'cat_feature1', 'cat_feature1',
    ],
    'value': ['val1', 'val2'],
    'mean': [1.25, 6.02],
    'stddev': [0.93, 8.59],
    'med': [1.20, 5.7],
    'q1': [0.50, 0.31],
    'q3': [2.34, 6.32],
    'whislo': [0.02, 0.11],
    'whishi': [2.50, 6.40]
})


class FeatureVisualizerTest(absltest.TestCase):

  def setUp(self):
    self.addCleanup(absltest.mock.patch.stopall)
    super(FeatureVisualizerTest, self).setUp()

    self.mock_bq_client = absltest.mock.create_autospec(bigquery.client.Client)
    self.features_table_path = 'project_id.dataset.features_table'
    self.label_column = 'label'
    self.numerical_features = ['num_feature1']
    self.categorical_features = ['cat_feature1']

  def test_plot_features_returns_correct_plots_for_binary_label(self):
    self.feature_viz_obj = feature_visualizer.FeatureVisualizer(
        bq_client=self.mock_bq_client,
        features_table_path=self.features_table_path,
        numerical_features=self.numerical_features,
        categorical_features=self.categorical_features,
        label_column=self.label_column,
        label_type='binary',
        positive_class_label='True',
        negative_class_label='False',
        num_pos_instances=10000,
        num_neg_instances=10000)

    self.mock_configure_sql = absltest.mock.patch.object(
        utils, 'read_file', autospec=True).start()

    self.mock_bq_client.query.return_value.to_dataframe.side_effect = [
        BINARY_LABEL_NUMERICAL_FEATURES_STATS,
        BINARY_LABEL_NUMERICAL_FEATURES_SAMPLE,
        BINARY_LABEL_CATEGORICAL_FEATURES_STATS
    ]

    label_values = ['False', 'True']

    num_feature1_data = BINARY_LABEL_NUMERICAL_FEATURES_STATS[
        BINARY_LABEL_NUMERICAL_FEATURES_STATS['feature'] == 'num_feature1']
    snapshot_dates_num_true = sorted(set(
        num_feature1_data[num_feature1_data['label'] == 'True']
        ['snapshot_date']))
    snapshot_dates_num_false = sorted(set(
        num_feature1_data[num_feature1_data['label'] == 'False']
        ['snapshot_date']))

    cat_feature1_data = BINARY_LABEL_CATEGORICAL_FEATURES_STATS[
        BINARY_LABEL_CATEGORICAL_FEATURES_STATS['feature'] == 'cat_feature1']
    cat_feature1_category_values1 = sorted(set(cat_feature1_data['value']))
    snapshot_dates_cat_true = sorted(set(
        cat_feature1_data[cat_feature1_data['label'] == 'True']
        ['snapshot_date']))
    snapshot_dates_cat_false = sorted(set(
        cat_feature1_data[cat_feature1_data['label'] == 'False']
        ['snapshot_date']))

    num_feature_1_plots, cat_feature_1_plots = (
        self.feature_viz_obj.plot_features())

    with self.subTest(name='test the number of plots returned'):
      self.assertLen(num_feature_1_plots, 3)
      self.assertLen(cat_feature_1_plots, 3)

    # Test elements in numerical fearture plots
    with self.subTest(name='test num_feature1 distribution box plot by label'):
      self.assertListEqual(label_values, [
          tick.get_text() for tick in num_feature_1_plots[0].get_yticklabels()
      ])
      self.assertEqual('Distribution of [num_feature1]',
                       num_feature_1_plots[0].get_title())

    with self.subTest(
        name='test snapshopt distribution of num_feature1 when label=True'):
      self.assertListEqual(
          snapshot_dates_num_true,
          sorted([
              tick.get_text()
              for tick in num_feature_1_plots[1].get_xticklabels()
          ]))
      self.assertEqual(
          'Snapshot-level distribution of [num_feature1] for label = True',
          num_feature_1_plots[1].get_title())

    with self.subTest(
        name='test snapshopt distribution of [num_feature1] when label=False'):
      self.assertListEqual(
          snapshot_dates_num_false,
          sorted([
              tick.get_text()
              for tick in num_feature_1_plots[2].get_xticklabels()
          ]))
      self.assertEqual(
          'Snapshot-level distribution of [num_feature1] for label = False',
          num_feature_1_plots[2].get_title())

    # Test elements in categorical fearture plots
    with self.subTest(name='test the cat_feature1 distribution plot'):
      self.assertListEqual(
          cat_feature1_category_values1,
          sorted([
              tick.get_text()
              for tick in cat_feature_1_plots[0].get_xticklabels()
          ]))
      self.assertEqual('Distribution of [cat_feature1]',
                       cat_feature_1_plots[0].get_title())

    with self.subTest(
        name='test snapshopt distribution of cat_feature1 when label=True'):
      self.assertListEqual(
          snapshot_dates_cat_true,
          sorted([
              tick.get_text()
              for tick in cat_feature_1_plots[1].get_xticklabels()
          ]))
      self.assertEqual(
          'Snapshot-level distribution of [cat_feature1] for label = True',
          cat_feature_1_plots[1].get_title())

    with self.subTest(
        name='test snapshopt distribution of [cat_feature1] when label=False'):
      self.assertListEqual(
          snapshot_dates_cat_false,
          sorted([
              tick.get_text()
              for tick in cat_feature_1_plots[2].get_xticklabels()
          ]))
      self.assertEqual(
          'Snapshot-level distribution of [cat_feature1] for label = False',
          cat_feature_1_plots[2].get_title())

  def test_plot_features_returns_correct_plots_for_numeric_label(self):
    self.feature_viz_obj = feature_visualizer.FeatureVisualizer(
        bq_client=self.mock_bq_client,
        features_table_path=self.features_table_path,
        numerical_features=self.numerical_features,
        categorical_features=self.categorical_features,
        label_column=self.label_column,
        label_type='numerical',
        num_instances=10000)

    self.mock_configure_sql = absltest.mock.patch.object(
        utils, 'read_file', autospec=True).start()

    self.mock_bq_client.query.return_value.to_dataframe.side_effect = [
        NUMERICAL_LABEL_NUMERICAL_FEATURES_STATS,
        NUMERICAL_LABEL_NUMERICAL_FEATURES_SAMPLE,
        NUMERICAL_LABEL_CATEGORICAL_FEATURES_STATS,
        NUMERICAL_LABEL_STATS
    ]

    num_feature_1_plots, cat_feature_1_plots = (
        self.feature_viz_obj.plot_features())

    with self.subTest(name='test the number of plots returned'):
      self.assertLen(num_feature_1_plots, 2)
      self.assertLen(cat_feature_1_plots, 2)

    # Test elements in numerical fearture plots
    num_snapshot_dates = sorted(set(
        NUMERICAL_LABEL_NUMERICAL_FEATURES_STATS['snapshot_date']))

    with self.subTest(name='test label vs num_feature1 scatter plot'):
      self.assertEqual(
          'Scatter plot of the Label vs [num_feature1] |  correlation = 0.71',
          num_feature_1_plots[0].get_title())

    with self.subTest(
        name='test snapshopt distribution of num_feature1'):
      self.assertListEqual(
          num_snapshot_dates,
          sorted([
              tick.get_text()
              for tick in num_feature_1_plots[1].get_xticklabels()
          ]))
      self.assertEqual(
          'Snapshot-level distribution of [num_feature1]',
          num_feature_1_plots[1].get_title())

    # Test elements in categorical fearture plots

    cat_feature1_values = list(
        NUMERICAL_LABEL_STATS[
            NUMERICAL_LABEL_STATS['feature'] == 'cat_feature1']['value'])

    cat_snapshot_dates = sorted(set(
        NUMERICAL_LABEL_CATEGORICAL_FEATURES_STATS[
            NUMERICAL_LABEL_CATEGORICAL_FEATURES_STATS[
                'feature'] == 'cat_feature1']['snapshot_date']))

    with self.subTest(name='test the label distribution plot'):
      self.assertListEqual(
          cat_feature1_values,
          sorted([
              tick.get_text()
              for tick in cat_feature_1_plots[0].get_xticklabels()
          ]))
      self.assertEqual(
          'Label distribution by [cat_feature1] categories',
          cat_feature_1_plots[0].get_title())

    with self.subTest(
        name='test snapshopt distribution of cat_feature1'):
      self.assertListEqual(
          cat_snapshot_dates,
          sorted([
              tick.get_text()
              for tick in cat_feature_1_plots[1].get_xticklabels()
          ]))
      self.assertEqual(
          'Snapshot-level distribution of [cat_feature1]',
          cat_feature_1_plots[1].get_title())

if __name__ == '__main__':
  absltest.main()
