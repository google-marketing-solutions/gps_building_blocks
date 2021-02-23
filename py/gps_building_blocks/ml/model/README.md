# Beta binomial model where the KPI is a rate [0, 1]

When we have a continuous outcome that is the ratio of successes to trials
(eg the conversion rate), we should not use linear regression but beta
regression.

This implements such beta binomial model in Tensorflow Probability.

Here is an example how to run the model

```python
import numpy as np
import tensorflow as tf
import tfp_beta_binomial

# some dummy data, we have 2 covariates
X = np.arange(6).reshape((3, 2))
successes = np.array([1, 2, 3])
trials = np.array([3, 4, 5])

bb_model = tfp_beta_binomial.BetaBinomialModel()
bb_model.fit(X, successes, trials)
print(bb_model) # will print parameter metrics
```

TODO(): expand readme with predictions.

The pre work was done by @axch, his
[colab](https://colab.corp.google.com/drive/1w2aaPsrLJjI3nrTWKLGPnU_asBryqiGu).




