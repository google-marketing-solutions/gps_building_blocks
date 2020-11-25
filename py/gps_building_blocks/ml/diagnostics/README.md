
# Machine Learning Diagnostics

**Disclaimer: This is not an official Google product.**

Machine Learning Diagnostics is a library designed to provide reliability and
scalability to the data science workflow.

## Table of Contents

-   [Key Features](#key-features)
-   [Modules overview](#modules-overview)
    -   [1. Bootstrap](#1-bootstrap)

## Key Features

1.  Reusable and flexible components.

## Modules overview

### Bootstrap
The module, `regression_bootstrap` offers a simple interface to run a
parallelized bootstrap analysis when using `sklearn.linear_model.LinearModel` models.
It is [good practice](https://statweb.stanford.edu/~owen/courses/305-1314/FoxOnBootingRegInR.pdf)
to use at least 1,000 bootstrap samples, even 10,000 is not uncommon.

### Example Usage

```python
  from sklearn import linear_model
  from gps_building_blocks.py.ml.diagnostics import bootstrap

  # Regressor for the bootstrap
  elastic_net = linear_model.ElasticNet(random_state=18)
  # Regressor to be used to tune the hyper-parameters
  elastic_net_cv = linear_model.ElasticNetCV(cv=3, random_state=18)

  # Kick-off the bootstrap process using all cores but one.
  result = bootstrap.regression_bootstrap(
      data, target,
      regressor=elastic_net, regressor_cv=elastic_net_cv,
      bootstraps=5,
      n_jobs=-1)
```
