# Beta binomial model where the KPI is a rate [0, 1]

When we have a continuous outcome that is the ratio of successes to trials
(eg the conversion rate), we should not use linear regression but a beta
binomial model.

A linear regression model could predict values outside the [0, 1] range but
this model avoids that. We prefer the beta binomial model over the beta model
as it can also handle edges cases (0 and 1).

This implements such beta binomial model in Tensorflow Probability (TFP). The
advantages of TFP are that you can run this on CPU or GPU to get good
performance.

This uses
[Hamiltonian Monte Carlo](https://en.wikipedia.org/wiki/Hamiltonian_Monte_Carlo)
for sampling.

## Why beta binomial

The beta binomial (BB) model has the advantage that it can also take into
account how many trials we had. 1 in 10 successes and 10 in 100 successes are
not the same.

$$y = successes / trials$$

$$\mu = logit^{-1}(\alpha + X \beta)$$

$$successes \sim BetaBinomial(trials, \mu * \kappa, (1 - \mu) * \kappa)$$

where we have a positive prior

$$\kappa \sim \Gamma(1, 1)$$

## Code

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
predicted_successes = bb_model.predict(X, trials)
```

You can decide how many chains (`nchains`) and iterations (`npost`) you want
to run.

## Inference

`print()` will give you parameter stats and rhat (around 1.0 means converged).
Since the $$\beta$$ parameters are inside an inverse logit, you can use the
`predict` function to create counterfactuals and read of the implied
change in successes or probability.

```python
X1 = np.array([[0, 1], [0, 2]]) # x0 is set to 0
X2 = np.array([[1, 1], [1, 2]]) # x0 is set to 1

pred1 = bb_model.predict(X1, trials)
pred2 = bb_model.predict(X2, trials)

change_in_successes = pred1 - pred2
change_in_prob = (pred1 - pred2) / trials
```

## Credits

The pre work was done by @axch.
