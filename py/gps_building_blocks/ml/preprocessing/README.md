


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

```python
import re
import tensorflow_hub as hub

from gps_building_blocks.py.ml.preprocessing.keyword_clustering import KeywordClustering

embed = hub.load("https://tfhub.dev/google/nnlm-en-dim50/2")
kwrd_clustering = KeywordClustering()

kw_clustering = keyword_clustering.KeywordClustering(model=embed)

tst_df = pd.DataFrame({'keyword':['car', 'engine', 'windshield wipers',
                                  'door handle', 'wheel', 'basketball',
                                  'football', 'soccer', 'baseball', 'hockey',
                                  'goal post']})
tst_df['phase_embed'] = tst_df.keyword.apply(kwrd_clustering.extract_embedding)
tst_df['avg_embed'] = tst_df.phase_embed.apply(
    kwrd_clustering.get_average_embedding
    )
tst_df_w_clusters, cluster_desc = kwrd_clustering.cluster_keywords(data=tst_df, colname_real='keyword', colname_mean_embed='avg_embed', n_clusters=2, num_of_closest_words=3)
```
