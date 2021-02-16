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

"""Beta binomial model in Tensorflow Probability."""

from typing import Any, List, Dict
import tensorflow.compat.v2 as tf
import tensorflow_probability as tfp

tfd = tfp.distributions
tfb = tfp.bijectors
root = tfd.JointDistributionCoroutine.Root


def beta_proportion(mu: tf.Tensor, kappa: tf.Tensor,
                    trials: tf.Tensor) -> tfp.distributions.BetaBinomial:
  """Returns Beta Binomial distribution.

  Check https://mc-stan.org/docs/2_19/functions-reference/beta-proportion-distribution.html. pylint: disable=line-too-long

  Args:
    mu: Probability of success, one dimenional tensor between 0 and 1.
    kappa: Scalar parameter > 0.
    trials: The number of trials, tensor same length as mu, integers as floats.

  Returns:
    TFP Beta Binomial distribution that can be sampled from.
  """
  return tfd.BetaBinomial(concentration1=mu*kappa, concentration0=(1-mu)*kappa,
                          total_count=trials)


def model(covariate_matrix: tf.Tensor,
          trials: tf.Tensor) -> tfp.distributions.JointDistributionCoroutine:
  """Return the beta regression model.

  Args:
    covariate_matrix: Matrix of covariates, two dimensional tensor.
    trials: The number of trials, one dimensional tensor, integers as floats.

  Returns:
    TFP distribution object given the data.
  """
  covariate_columns = covariate_matrix.shape[-1]

  @tfd.JointDistributionCoroutine
  def _model_it():
    beta = yield root(tfd.Sample(tfd.Normal(0, 1.0),
                                 [covariate_columns], name='beta'))
    alpha = yield root(tfd.Normal(0, 1.0, name='alpha'))
    kappa = yield root(tfd.Gamma(1, 1, name='kappa'))
    mu = tf.math.sigmoid(alpha[..., tf.newaxis] + tf.einsum('...p,np->...n',
                                                            beta,
                                                            covariate_matrix))
    ct = yield tfd.Independent(beta_proportion(mu, kappa[..., tf.newaxis],
                                               trials),
                               reinterpreted_batch_ndims=1,
                               name='ct')
  return _model_it


def _trace_fn(state: Any, kernel_results: Any, *reduction_results) -> Dict:
  """Returns state history, step size, leapfrogs, trajectory length and rhat."""
  try:
    leapfrogs = kernel_results.inner_results.inner_results.leapfrogs_taken
    length = []
  except AttributeError:
    leapfrogs = []
    length = kernel_results.inner_results.inner_results.max_trajectory_length
  return {'state': state,
          'step size': kernel_results.new_step_size,
          'leapfrogs taken': leapfrogs,
          'trajectory length': length,
          'r-hat': reduction_results}


def initialize(pinned: Any, nchains: int, seed: List[int] = None) -> Any:
  """Return initialized state.

  Args:
    pinned: Pinned TFP model object.
    nchains: Number of chains.
    seed: List of 2 int seeds.

  Returns:
    Initialized state.
  """
  init_dist = tfp.experimental.mcmc.init_near_unconstrained_zero(pinned)
  init_state = list(tfp.experimental.mcmc.retry_init(
      init_dist.sample,
      target_fn=pinned.unnormalized_log_prob,
      sample_shape=nchains,
      seed=seed))
  return init_state


@tf.function
def fit_dataset(covariate_matrix: tf.Tensor, successes: tf.Tensor,
                trials: tf.Tensor, nchains: int, nadapt: int,
                nburn: int, npost: int, seed: List[int] = None) -> Any:
  """Estimate the model given the data.

  Args:
    covariate_matrix: Matrix of covariates, two dimensional tensor.
    successes: Number of successes, one dimensional tensor, integer as float.
    trials: Number of trials, one dimensional tensor, integer as float.
    nchains: Number of chains.
    nadapt: Number of adaption steps.
    nburn: Number of burnin draws.
    npost: Number of posterior draws.
    seed: List of 2 int seeds.

  Returns:
    final_state and trace from estimated model.
  """
  if successes.shape != trials.shape:
    raise AssertionError(successes.shape, 'is not', trials.shape)
  init_seed, sample_seed = tfp.random.split_seed(n=2, seed=seed)

  pinned = model(covariate_matrix, trials).experimental_pin(ct=successes)

  init_state = initialize(pinned, nchains, seed=init_seed)
  # TODO(): make step_size a parameter.
  kernel = tfp.mcmc.HamiltonianMonteCarlo(pinned.unnormalized_log_prob,
                                          step_size=0.05, num_leapfrog_steps=1)
  kernel = tfp.experimental.mcmc.GradientBasedTrajectoryLengthAdaptation(
      inner_kernel=kernel, num_adaptation_steps=nadapt)
  # [beta, alpha, kappa].
  bijectors = [tfb.Identity(), tfb.Identity(), tfb.Softplus(low=1e-6)]
  kernel = tfp.mcmc.TransformedTransitionKernel(kernel, bijectors)
  kernel = tfp.mcmc.DualAveragingStepSizeAdaptation(kernel,
                                                    num_adaptation_steps=nadapt)

  def _sample():
    fit = tfp.experimental.mcmc.sample_chain(
        num_results=nadapt + nburn + npost,
        current_state=init_state,
        kernel=kernel,
        reducer=tfp.experimental.mcmc.PotentialScaleReductionReducer(),
        trace_fn=_trace_fn,
        seed=sample_seed)
    return fit.final_state, fit.trace

  return _sample()


def inv_logit(x: tf.Tensor) -> tf.Tensor:
  """Return inverse logit of x."""
  return tf.math.exp(x) / (1 + tf.math.exp(x))

# TODO(): create user friendly estimator class, hides above complexity.
