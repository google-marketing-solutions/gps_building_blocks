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
"""Functions to cluster words/phrase/sentences using embedding."""

from importlib import resources
from typing import List, Optional, Text, Tuple

import numpy as np
from numpy import linalg
import pandas as pd
from sklearn import cluster
import tensorflow as tf
import tensorflow_hub as hub

from gps_building_blocks.ml.preprocessing import data as preprocess_data


class KeywordClustering(object):
  """Class to cluster text using embeddings of word/phrase or sentences."""

  def __init__(self,
               model: Optional[tf.keras.Model] = None,
               stopwords: Optional[List[Text]] = None) -> None:
    """Initialize embed model and list of stopwords.

    Args:
      model: Pretrained model object for text embedding.
        All pre trained tf embeddings:
          https://tfhub.dev/s?module-type=text-embedding
      stopwords: Stopwords to remove from embedding.

    Attributes:
      k_means: cluster.KMeans object used to cluster keywords.
    """
    if model is None:
      self.model = hub.load("https://tfhub.dev/google/nnlm-en-dim50/2")
    else:
      self.model = model

    if stopwords is None:
      stopwords_default = resources.read_text(preprocess_data,
                                              "stopwords_eng.txt")
      stopwords_default = stopwords_default.split("\n")[1:]
      self.stopwords_to_remove = list(
          filter(lambda word: word, stopwords_default))
    else:
      self.stopwords_to_remove = stopwords

    self.k_means = cluster.KMeans

  def extract_embedding(self, phrase: str) -> np.ndarray:
    """Extracts embedding of phrase using pretrained embedding model.

    Args:
      phrase: Word, phrase or sentence input for embedding model.

    Returns:
      Array of embedding for each word in phrase.
    """
    phrase_input = [
        i.lower()
        for i in phrase.split(" ")
        if i not in self.stopwords_to_remove
    ]
    embed_phrase = self.model(phrase_input).numpy()
    return embed_phrase

  def get_average_embedding(self,
                            phrase_embed: np.ndarray) -> np.ndarray:
    """Calculates average embedding from embeddings of each word.

    Args:
      phrase_embed: Array of each word's embedding in phrase, output from
        extract_embedding.

    Returns:
      Array mean of word (phrase).
    """
    return np.mean(phrase_embed, axis=0)

  def cluster_keywords(
      self,
      data: pd.DataFrame,
      colname_real: str,
      colname_mean_embed: str,
      n_clusters: int,
      num_of_closest_words: int = 2) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Clusters words using K-Means into num_clusters clusters.

    Args:
      data: A pd.DataFrame with words and average embedding.
      colname_real: Column name for column of original keywords.
      colname_mean_embed: Column name for column of average text embeddings.
      n_clusters: Number of clusters.
      num_of_closest_words: Number of words selected for cluster description.

    Returns:
      Two dataframes
      First dataframe is original data with cluster label column and distance
        to center column.
      Second dataframe contains cluster label and num_of_closest_words for each
        cluster.
    """
    entityname_matrix = pd.DataFrame.from_records(data[colname_mean_embed])
    k_means = self.k_means()
    k_means.n_clusters = n_clusters
    k_means = k_means.fit(entityname_matrix)
    data["labels"] = k_means.labels_

    # Calculate normalized distance of each point from its cluster center
    data["center_diff"] = np.nan

    for i in range(0, n_clusters):
      dist_from_cluster_center = data[data["labels"] == i][
          colname_mean_embed].apply(lambda x: x - k_means.cluster_centers_[i])
      data.loc[data["labels"] == i, "center_diff"] = linalg.norm(
          dist_from_cluster_center.to_list(), axis=1)

    # pick out num_of_closest_words closest words to center to describe cluster
    closest = data.groupby("labels")["center_diff"].nsmallest(
        num_of_closest_words)
    data_cluster_description = data.loc[closest.index.get_level_values(level=1)]
    data_cluster_description = data_cluster_description.groupby(
        ["labels"], as_index=False).agg({colname_real: ", ".join})
    return data, data_cluster_description
