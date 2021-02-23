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

"""Tests for gps_building_blocks.ml.model.tfp_beta_binomial."""

import numpy as np
import tensorflow.compat.v2 as tf
import tensorflow_probability as tfp
from gps_building_blocks.ml.model import tfp_beta_binomial
from absl.testing import absltest


class TfpBetaBinomialTest(absltest.TestCase):

  def test_beta_proportion(self):
    mu = np.array([.5, .5])
    kappa = np.array([1.])
    trials = np.array([1., 1.])
    bb = tfp_beta_binomial.beta_proportion(mu, kappa, trials)
    self.assertIsInstance(bb, tfp.distributions.BetaBinomial)

  def test_model(self):
    covariate_matrix = np.zeros((2, 2), dtype=np.float32)
    trials = np.array([1., 1.], dtype=np.float32)
    pinned = tfp_beta_binomial.model(covariate_matrix, trials)
    self.assertIsInstance(pinned, tfp.distributions.JointDistributionCoroutine)

  def test_with_synthetic_data(self):
    rows, cols = 100, 2
    nchains = 3
    nadapt, nburn, npost = 10, 10, 10
    covariate_matrix = tf.random.stateless_normal([rows, cols], seed=[5, 8])
    rng = np.random.RandomState(seed=59)
    # model is prob = inv_logit(1 + x1 + noise).
    prob = tfp_beta_binomial.inv_logit(1 + covariate_matrix[:, 1]
                                       + rng.normal(size=rows))
    trials = tf.random.stateless_poisson([rows], seed=[6, 9], lam=100,
                                         dtype=np.float32)
    successes = tf.math.round(prob * trials)
    _, trace = tfp_beta_binomial.fit_dataset(covariate_matrix, successes,
                                             trials, nchains, nadapt, nburn,
                                             npost)
    beta, alpha, kappa = trace['state']
    self.assertTupleEqual(tuple(beta.shape),
                          (nadapt + nburn + npost, nchains, cols))
    self.assertTupleEqual(tuple(alpha.shape), (nadapt + nburn + npost, nchains))
    self.assertTupleEqual(tuple(kappa.shape), (nadapt + nburn + npost, nchains))

  def test_estimator_with_data_check_posterior(self):
    # generate some numpy test data (as opposed to tensors).
    rows, cols, npost, nchains = 5, 2, 100, 2
    covariate_matrix = np.arange(rows * cols).reshape((rows, cols)) / 10
    successes = np.arange(rows)
    trials = 2 * successes
    bb_model = tfp_beta_binomial.BetaBinomialModel(npost=npost, nchains=nchains)
    bb_model.fit(covariate_matrix, successes, trials)
    posterior_params = bb_model.extract_posterior_parameters()
    print(bb_model)
    self.assertTupleEqual(posterior_params['beta'].shape,
                          (npost * nchains, cols))


if __name__ == '__main__':
  absltest.main()
