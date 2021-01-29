# Lint as: python3
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
"""Tests for keyword_clustering."""

import importlib_resources
import numpy as np
import pandas as pd
import tensorflow as tf

from absl.testing import absltest
from absl.testing.absltest import mock
from gps_building_blocks.ml.preprocessing import data as preprocess_data
from gps_building_blocks.ml.preprocessing import keyword_clustering


class KeywordClusteringTest(absltest.TestCase):

  @classmethod
  def setUpClass(cls):
    super(KeywordClusteringTest, cls).setUpClass()
    cls.phrase = "the hello world"
    cls.phrase_embedding = np.full((2, 50), 0.5)
    cls.phrase_embedding_avg = np.full(50, 0.5)
    cls.model = mock.MagicMock(
        side_effect=lambda x: tf.constant(0.5, shape=(len(x), 50)))

    cls.cluster_output_shape = (11, 5)
    cls.cluster_description_output_shape = (2, 2)

    cls.kw_clustering = keyword_clustering.KeywordClustering(
        model=cls.model)

    contents = importlib_resources.read_text(preprocess_data,
                                             "example_cluster_df.txt")
    contents = contents.split("\n")[1]
    cls.test_df = pd.read_json(contents)

  def test_extract_and_average_embedding(self):
    phrase = self.phrase

    phrase_embed = self.kw_clustering.extract_embedding(phrase=phrase)
    phrase_embed_avg = self.kw_clustering.get_average_embedding(phrase_embed)

    np.testing.assert_array_equal(phrase_embed, self.phrase_embedding)
    np.testing.assert_array_equal(phrase_embed_avg, self.phrase_embedding_avg)

  def test_cluster_keywords_output_shape(self):
    data = self.test_df

    data_output, cluster_description = self.kw_clustering.cluster_keywords(
        data=data,
        colname_real="keyword",
        colname_mean_embed="avg_embed",
        n_clusters=2,
        num_of_closest_words=3)

    self.assertEqual(data_output.shape, self.cluster_output_shape)
    self.assertEqual(cluster_description.shape,
                     self.cluster_description_output_shape)


if __name__ == "__main__":
  absltest.main()
