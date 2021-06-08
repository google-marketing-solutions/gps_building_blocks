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

NUMERICAL_FEATURES_STATS = pd.DataFrame({
    'snapshot_date': [
        '2015-01-05', '2015-01-06', '2015-01-07', '2015-01-08', '2015-01-09'
    ],
    'feature': [
        'num_feature1', 'num_feature1', 'num_feature1', 'num_feature2',
        'num_feature2'
    ],
    'label': [False, True, False, True, False],
    'record_count': [56271, 123, 51632, 98, 47410],
    'prop_missing': [0.5, 0.3, 0.0, 0.0, 0.0],
    'prop_non_num': [0.0, 0.0, 0.0, 0.0, 0.0],
    'average': [1.25, 6.02, 1.24, 5.20, 1.22],
    'stddev': [0.93, 8.59, 0.90, 4.71, 0.86]
})

NUMERICAL_FEATURES_SAMPLE = pd.DataFrame({
    'num_feature1': [8, 10, 5, 20, 2],
    'num_feature2': [100, 50, 25, 120, 23],
    'label': [False, True, False, True, False]
})

CATEGORICAL_STATS = {
    'snapshot_date': [
        '2017-01-05 00:00:00', '2017-01-05 00:00:00', '2017-01-05 00:00:00',
        '2017-01-05 00:00:00', '2017-01-05 00:00:00'
    ],
    'feature': [
        'cat_feature1', 'cat_feature2', 'cat_feature1', 'cat_feature2',
        'cat_feature2'
    ],
    'value': ['Chrome', 'Macintosh', 'Chrome', 'Macintosh', 'Chrome OS'],
    'label': [True, True, False, False, False],
    'count': [113, 74, 38749, 13834, 1647],
    'total': [123, 123, 56271, 56271, 56271],
    'percentage': [
        91.869918699187, 60.16260162601627, 68.86140285404561,
        24.58459952728759, 2.9269072879458338
    ]
}

CATEGORICAL_FEATURES_STATS = pd.DataFrame(CATEGORICAL_STATS)


class FeatureVisualizerTest(absltest.TestCase):

  def setUp(self):
    self.addCleanup(absltest.mock.patch.stopall)
    super(FeatureVisualizerTest, self).setUp()

    self.mock_bq_client = absltest.mock.create_autospec(bigquery.client.Client)
    self.features_table_path = 'project_id.dataset.features_table'
    self.numerical_features = ('num_feature1', 'num_feature2')
    self.categorical_features = ('cat_feature1', 'cat_feature2')
    self.label_column = 'predictionLabel',
    self.positive_class_label = True,
    self.negative_class_label = False,
    self.num_pos_instances = 10000,
    self.num_neg_instances = 10000

    self.feature_viz_obj = feature_visualizer.FeatureVisualizer(
        bq_client=self.mock_bq_client,
        features_table_path=self.features_table_path,
        numerical_features=self.numerical_features,
        categorical_features=self.categorical_features,
        label_column=self.label_column,
        positive_class_label=self.positive_class_label,
        negative_class_label=self.negative_class_label,
        num_pos_instances=self.num_pos_instances,
        num_neg_instances=self.num_neg_instances)

    self.mock_configure_sql = absltest.mock.patch.object(
        utils, 'read_file', autospec=True).start()

    self.mock_bq_client.query.return_value.to_dataframe.side_effect = [
        NUMERICAL_FEATURES_STATS, NUMERICAL_FEATURES_SAMPLE,
        CATEGORICAL_FEATURES_STATS
    ]

  def test_plot_features_returns_correct_plots(self):
    num_feature1_data = NUMERICAL_FEATURES_STATS[
        NUMERICAL_FEATURES_STATS['feature'] == 'num_feature1']
    num_feature1_average_true = list(
        num_feature1_data[num_feature1_data['label']]['average'])
    num_feature1_average_false = list(
        num_feature1_data[~num_feature1_data['label']]['average'])

    cat_feature1_data = CATEGORICAL_FEATURES_STATS[
        CATEGORICAL_FEATURES_STATS['feature'] == 'cat_feature1']
    cat_feature1_category_values1 = sorted(set(cat_feature1_data['value']))
    cat_feature1_category_values2 = sorted(set(pd.to_datetime(
        cat_feature1_data['snapshot_date']).dt.date.astype(str)))

    num_feature_1_plots, _, cat_feature_1_plots, _ = (
        self.feature_viz_obj.plot_features())

    with self.subTest(name='test the number of plots returned'):
      self.assertLen(num_feature_1_plots, 3)
      self.assertLen(cat_feature_1_plots, 3)

    with self.subTest(name='test the title of the distribution by label'):
      self.assertEqual('Distribution of [num_feature1]',
                       num_feature_1_plots[0].get_title())

    with self.subTest(
        name='test the elements of average plot of num_feature_1 label=True'):
      self.assertListEqual(
          num_feature1_average_true,
          list(num_feature_1_plots[1].get_lines()[0].get_data()[1]))

    with self.subTest(
        name='test the elements of average plot of num_feature_1 label=False'):
      self.assertListEqual(
          num_feature1_average_false,
          list(num_feature_1_plots[2].get_lines()[0].get_data()[1]))

    with self.subTest(
        name='test the elements of distribution of cat_feature_1'):
      self.assertListEqual(
          cat_feature1_category_values1,
          sorted([
              tick.get_text()
              for tick in cat_feature_1_plots[0].get_xticklabels()
          ]))

    with self.subTest(
        name='test the elements of snapshot distribution of cat_feature_1'):
      self.assertListEqual(
          cat_feature1_category_values2,
          sorted([
              tick.get_text()
              for tick in cat_feature_1_plots[1].get_xticklabels()
          ]))

    with self.subTest(name='test title of the distribution of cat_feature_1'):
      self.assertEqual('Distribution of [cat_feature1]',
                       cat_feature_1_plots[0].get_title())


if __name__ == '__main__':
  absltest.main()
