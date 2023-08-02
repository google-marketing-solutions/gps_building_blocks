# Copyright 2020 Google LLC
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
"""Tests for google3.corp.gtech.ads.ds_utils.feature_graph_visualization."""

import networkx as nx
import numpy as np
import pandas as pd
import pandas.testing as pandas_testing
from sklearn import datasets

from absl.testing import absltest
from gps_building_blocks.analysis import feature_graph_visualization as fgv


class FeatureGraphVisualizationTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    iris = datasets.load_iris()
    iris_df = pd.DataFrame(data=np.c_[iris['data'], iris['target']],
                           columns=iris['feature_names']+['target'])
    self.correlation = iris_df[iris['feature_names']].corr()
    features = self.correlation.columns
    self.threshold = 0.3
    corr_matrix = np.array(self.correlation)
    inds = np.argwhere(abs(np.tril(corr_matrix, -1)) > self.threshold)
    linked_features = [(features[i1], features[i2]) for i1, i2 in inds]
    self.num_edges = len(linked_features)
    self.feature_graph = nx.Graph()
    self.feature_graph.add_edges_from(linked_features)

  def test_edge_colors(self):
    expected_edge_colors = [
        'red' if self.correlation.loc[edge[0], edge[1]] > 0 else 'blue'
        for edge in self.feature_graph.edges()
    ]

    fig = fgv.feature_graph_visualization(self.correlation,
                                          threshold=self.threshold)

    self.assertEqual(
        expected_edge_colors,
        [fig.data[i]['line']['color'] for i in range(self.num_edges)])

  def test_edge_widths(self):
    expected_edge_widths = [
        abs(self.correlation.loc[edge[0], edge[1]]) * 5
        for edge in self.feature_graph.edges()
    ]

    fig = fgv.feature_graph_visualization(self.correlation,
                                          threshold=self.threshold)

    self.assertEqual(
        expected_edge_widths,
        [fig.data[i]['line']['width'] for i in range(self.num_edges)])

  def test_correlation_matrix_error(self):
    wrong_correlation = self.correlation.drop(self.correlation.index[0])
    with self.assertRaises(ValueError):
      fgv.feature_graph_visualization(wrong_correlation,
                                      threshold=self.threshold)

# TODO (): parameterize and test output colormap
  def test_other_colormaps(self):
    fgv.feature_graph_visualization(
        self.correlation, threshold=self.threshold, color_map='coolwarm')

  def test_cluster_to_sim(self):
    cluster = {'A': 1,
               'B': 2,
               'C': 1,
               'D': 3,
               'E': 2}
    similarity = fgv.cluster_to_sim(cluster)
    expected_output = pd.DataFrame(
        np.array([[1, 0, 1, 0, 0], [0, 1, 0, 0, 1], [1, 0, 1, 0, 0],
                  [0, 0, 0, 1, 0], [0, 1, 0, 0, 1]]),
        columns=['A', 'B', 'C', 'D', 'E'],
        index=['A', 'B', 'C', 'D', 'E'])
    pandas_testing.assert_frame_equal(similarity, expected_output)

if __name__ == '__main__':
  absltest.main()
