


# Preprocessing

**Disclaimer: This is not an official Google product.**

Preprocessing is a module that includes tools for preprocessing of machine
learning data.

## Table of Contents

- [How to use?](#how-to-use)
  * [1. Variance Inflation Factors (VIF)](#1-variance-inflation-factors-vif)
  * [2. Keyword Clustering using TF Embedding](#2-kw-clustering-using-tf-embedding)

## How to use?

### 1. Variance Inflation Factors (VIF)

Function to calculate the VIFs for the columns in pandas DataFrame.

```python
from gps_building_blocks.py.ml.preprocessing import vif

vif_df = vif.calculate_vif(data_df, sort=False)
```

### 2. Keyword Clustering using TF Embedding

The following module is a way to cluster a group words, phrase or sentences
using Tensorflow text embeddings which represents texts as numbers. Potential
use cases might be:

1. Keyword clustering for bidding purposes
2. Feature reduction in creative models (SACA, Creative Drivers) by clustering
similar objects into single feature.

All pretrained TF embedding models can be found in [TF hub] (https://tfhub.dev/s?module-type=text-embedding)

WIP: Still to add clustering algorithm

```python
import re
import tensorflow_hub as hub

from gps_building_blocks.py.ml.preprocessing import keyword_clustering

embed = hub.load("https://tfhub.dev/google/nnlm-en-dim50/2")
phrase = 'hello world the'

kw_clustering = keyword_clustering.KeywordClustering(model=embed)

phrase_embed = KeywordClustering.extract_tf_embedding(phrase=phrase)
phrase_embed_avg = KeywordClustering.get_average_embedding(
    phrase_embed=phrase_embed
    )
# WIP - Adding clustering
```
