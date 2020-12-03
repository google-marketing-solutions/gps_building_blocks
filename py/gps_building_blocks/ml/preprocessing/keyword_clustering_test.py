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

import unittest
from unittest import mock
import numpy as np
import tensorflow as tf

from gps_building_blocks.ml.preprocessing import keyword_clustering


class KeywordClusteringTest(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    super(KeywordClusteringTest, cls).setUpClass()
    cls.phrase = 'the hello world'
    cls.phrase_embedding = np.full((2, 50), 0.5)
    cls.phrase_embedding_avg = np.full(50, 0.5)
    cls.model = mock.MagicMock(
        side_effect=lambda x: tf.constant(0.5, shape=(len(x), 50))
        )

  def test_extract_and_average_embedding(self):
    kw_clustering = keyword_clustering.KeywordClustering(
        model=self.model)
    phrase_embed = kw_clustering.extract_embedding(
        phrase=self.phrase
        )
    phrase_embed_avg = kw_clustering.get_average_embedding(phrase_embed)
    np.testing.assert_array_equal(phrase_embed, self.phrase_embedding)
    np.testing.assert_array_equal(phrase_embed_avg, self.phrase_embedding_avg)

if __name__ == '__main__':
  unittest.main()
