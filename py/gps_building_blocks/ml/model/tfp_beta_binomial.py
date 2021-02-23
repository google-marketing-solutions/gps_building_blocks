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

from typing import Any, Callable, Dict, List
import numpy as np
import tensorflow.compat.v2 as tf
import tensorflow_probability as tfp

tfd = tfp.distributions
tfb = tfp.bijectors
root = tfd.JointDistributionCoroutine.Root


def beta_proportion(mu: tf.Tensor,
                    kappa: tf.Tensor,
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
    #TODO(): Allow user to change priors.
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


def _trace_fn(state: Any,
              kernel_results: Any,
              *reduction_results) -> Dict:
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


def initialize(pinned: Any,
               nchains: int,
               seed: List[int] = None) -> Any:
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
def fit_dataset(covariate_matrix: tf.Tensor,
                successes: tf.Tensor,
                trials: tf.Tensor,
                nchains: int,
                nadapt: int,
                nburn: int,
                npost: int,
                seed: List[int] = None) -> Any:
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


class BetaBinomialModel():
  """Beta-Binomial estimator."""

  def __init__(self,
               nchains: int = 2,
               nadapt: int = 100,
               nburn: int = 100,
               npost: int = 100,
               prob: List[float] = [.05, .95]):
    """Initialize the estimator.

    Args:
      nchains: Number of chains.
      nadapt: Number of adaption steps.
      nburn: Number of burnin draws.
      npost: Number of posterior draws.
      prob: Credible interval quantiles, list of upper and lower,
        between 0 and 1.

    Additional attributes:
      trace: Holds the trace from the HMC run.
      state: Holds final state from HMC run.
      dtype: TF data type for tensors.
    """
    self.nchains = nchains
    self.nadapt = nadapt
    self.nburn = nburn
    self.npost = npost
    self.prob = prob
    self.trace = None
    self.state = None
    self.dtype = tf.float32
    self._covariate_columns = 0

  def fit(self,
          covariate_matrix: np.ndarray,
          successes: np.ndarray,
          trials: np.ndarray,
          seed: List[int] = None) -> None:
    """Fit the model given the data.

    Note that this takes numpy arrays and converts them to tensors.

    Args:
      covariate_matrix: Matrix of covariates, two dimensional array.
      successes: Number of successes, one dimensional array.
      trials: Number of trials, one dimensional array.
      seed: List of 2 int seeds.
    """
    self._covariate_columns = covariate_matrix.shape[-1]
    covariate_matrix = tf.convert_to_tensor(covariate_matrix, dtype=self.dtype)
    successes = tf.convert_to_tensor(successes, dtype=self.dtype)
    trials = tf.convert_to_tensor(trials, dtype=self.dtype)
    self.state, self.trace = fit_dataset(covariate_matrix, successes, trials,
                                         self.nchains, self.nadapt, self.nburn,
                                         self.npost, seed=seed)

  def predict(self,
              covariate_matrix: np.ndarray,
              trials: np.ndarray,
              aggfunc: Callable = np.mean) -> np.ndarray:
    """Predict the number of successes."""
    # TODO(): implement sampled predictions.
    return np.array([0])

  def extract_posterior_parameters(self) -> Dict[str, np.ndarray]:
    """Return posterior draws for all parameters."""
    beta, alpha, kappa = self.trace['state']
    # get rid of chain dimension.
    beta = beta.numpy()[-self.npost:, :, :]
    beta = beta.reshape((self.npost * self.nchains, self._covariate_columns))
    alpha = alpha.numpy()[-self.npost:, :].flatten()
    kappa = kappa.numpy()[-self.npost:, :].flatten()
    return {'beta': beta, 'alpha': alpha, 'kappa': kappa}

  def __str__(self) -> str:
    """Print coefficient stats (quantile, mean, r-hat)."""
    beta, alpha, kappa = self.trace['state']
    rhats = self.trace['r-hat'][0]
    prob = self.prob
    # Keep only the last npost steps for inference.
    # Shape is (steps, chains, size).
    beta = beta.numpy()[-self.npost:, :, :]
    alpha = alpha.numpy()[-self.npost:, :]
    kappa = kappa.numpy()[-self.npost:, :]
    rhat_beta = rhats[0].numpy()[-self.npost:, :]
    rhat_alpha = rhats[1].numpy()[-self.npost:]
    rhat_kappa = rhats[2].numpy()[-self.npost:]
    report = [f'{len(alpha)} samples']
    report.append(f'using {prob[1] - prob[0]:.2f} interval')
    report.append('Param. CI-Low Mean CI-Up Rhat')
    for i in range(self._covariate_columns):
      param = beta[:, :, i]
      report.append((f'beta{i} {np.quantile(param, prob[0]):.2f} '
                     f'{param.mean():.2f} '
                     f'{np.quantile(param, prob[1]):.2f} '
                     f'{rhat_beta[:, i].mean():.2f}'))
    report.append((f'alpha {np.quantile(alpha, prob[0]):.2f} '
                   f'{alpha.mean():.2f} '
                   f'{np.quantile(alpha, prob[1]):.2f} '
                   f'{rhat_alpha.mean():.2f}'))
    report.append((f'kappa {np.quantile(kappa, prob[0]):.2f} '
                   f'{kappa.mean():.2f} '
                   f'{np.quantile(kappa, prob[1]):.2f} '
                   f'{rhat_kappa.mean():.2f}'))
    return '\n'.join(report)

