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
"""Functions to cluster words/phrase/sentences using embedding."""

import os
from typing import List, Text, Optional

import numpy as np
import tensorflow as tf
import tensorflow_hub as hub

from google3.pyglib import resources

CURR_PATH = os.path.dirname(__file__)
DATA_PATH = os.path.join(CURR_PATH, "data")


class KeywordClustering(object):
  """Class to cluster text using embeddings of word/phrase or sentences."""
# TODO() Add clustering function

  def __init__(self,
               model: Optional[tf.keras.Model] = None,
               stopwords: Optional[List[Text]] = None
               ) -> None:
    """Initialize embed model and list of stopwords.

    Args:
      model: Pretrained model object for text embedding.
        All pre trained tf embeddings:
          https://tfhub.dev/s?module-type=text-embedding
      stopwords: Stopwords to remove from embedding.
    """
    if model is None:
      self.model = hub.load("https://tfhub.dev/google/nnlm-en-dim50/2")
    else:
      self.model = model
    if stopwords is None:
      stopwords_default = resources.GetResource(
          os.path.join(DATA_PATH, "stopwords_eng.txt"), "r")
      self.stopwords_to_remove = stopwords_default.split("\n")[1:]
    else:
      self.stopwords_to_remove = stopwords

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
    """Calculates the avg embedding from embeddings of each word (phrase_embed).

    Args:
      phrase_embed: Array of each word's embedding in phrase, output from
        extract_embedding.

    Returns:
      Array mean of word (phrase).
    """
    embed_phrase_avg = np.mean(phrase_embed, axis=0)
    return embed_phrase_avg

