


# Preprocessing

**Disclaimer: This is not an official Google product.**

Preprocessing is a module that includes tools for preprocessing of machine
learning data.

## Table of Contents

- [How to use?](#how-to-use)
  * [1. Variance Inflation Factors (VIF)](#1-variance-inflation-factors-vif)

## How to use?

### 1. Variance Inflation Factors (VIF)

Function to calculate the VIFs for the columns in pandas DataFrame.

```python
from gps_building_blocks.py.ml.preprocessing import vif

vif_df = vif.calculate_vif(data_df, sort=False)
```
