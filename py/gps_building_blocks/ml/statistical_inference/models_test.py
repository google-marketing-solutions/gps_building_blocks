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

"""Tests for gps_building_blocks.ml.statistical_inference.models."""

import numpy as np
import pandas as pd

from gps_building_blocks.ml.statistical_inference import data_preparation
from gps_building_blocks.ml.statistical_inference import models
from absl.testing import absltest


def _prepare_data_and_target(ready_for_modelling=True):
  # Prepare data
  data = np.array(
      [[0.496714150, -0.13826430, 0.647688540, 1.523029860, -0.23415337],
       [-0.23413696, 1.579212820, 0.767434730, -0.46947439, 0.542560040],
       [-0.46341769, -0.46572975, 0.241962270, -1.91328024, -1.72491783],
       [-0.56228753, -1.01283112, 0.314247330, -0.90802408, -1.41230370],
       [1.465648770, -0.22577630, 0.067528200, -1.42474819, -0.54438272],
       [0.110922590, -1.15099358, 0.375698020, -0.60063869, -0.29169375],
       [-0.60170661, 1.852278180, -0.01349722, -1.05771093, 0.822544910],
       [-1.22084365, 0.208863600, -1.95967012, -1.32818605, 0.196861240],
       [0.738466580, 0.171368280, -0.11564828, -0.30110370, -1.47852199],
       [-0.71984421, -0.46063877, 1.057122230, 0.343618290, -1.76304016],
       [0.324083970, -0.38508228, -0.67692200, 0.611676290, 1.030999520],
       [0.931280120, -0.83921752, -0.30921238, 0.331263430, 0.975545130],
       [-0.47917424, -0.18565898, -1.10633497, -1.19620662, 0.812525820],
       [1.356240030, -0.07201012, 1.003532900, 0.361636030, -0.64511975],
       [0.361395610, 1.538036570, -0.03582604, 1.564643660, -2.61974510],
       [0.821902500, 0.087047070, -0.29900735, 0.091760780, -1.98756891],
       [-0.21967189, 0.357112570, 1.477894040, -0.51827022, -0.80849360],
       [-0.50175704, 0.915402120, 0.328751110, -0.52976020, 0.513267430],
       [0.097077550, 0.968644990, -0.70205309, -0.32766215, -0.39210815],
       [-1.46351495, 0.296120280, 0.261055270, 0.005113460, -0.23458713]])
  # Decreasing coefficients with alternated signs
  idx = np.arange(data.shape[1])
  coefficients = (-1) ** idx * np.exp(-idx / 10)
  coefficients[10:] = 0  # sparsify
  target = np.dot(data, coefficients)
  # Add noise
  noise = np.array(
      [0.496714150, -0.13826430, 0.64768854, 1.523029860, -0.23415337,
       -0.23413696, 1.579212820, 0.76743473, -0.46947439, 0.542560040,
       -0.46341769, -0.46572975, 0.24196227, -1.91328024, -1.72491783,
       -0.56228753, -1.01283112, 0.31424733, -0.90802408, -1.41230370])
  target += 0.01 * noise

  data = pd.DataFrame(data)
  data['target'] = target
  inference_data = data_preparation.InferenceData(data, 'target')

  if ready_for_modelling:
    inference_data._has_control_factors = True
    inference_data._checked_low_variance = True
    inference_data._checked_collinearity = True

  return inference_data


class LinearModelTest(absltest.TestCase):

  def test_fit(self):
    data = _prepare_data_and_target()
    model = models.InferenceElasticNet(random_state=18)
    expected_result = pd.DataFrame(
        data=[[-0.203832],
              [-0.134636],
              [0.0108217],
              [0.0100611],
              [0.0000000],
              [0.0000000],],
        columns=['effect'],
        index=[1, 'Intercept', 0, 4, 3, 2])

    model.fit(data)
    result = model.get_results()

    pd.testing.assert_frame_equal(
        result[['effect']],
        expected_result,
        check_less_precise=2,
        check_index_type=False)

  def test_fit_bootstrap(self):
    data = _prepare_data_and_target()
    model = models.InferenceElasticNet(random_state=18)
    expected_result = pd.DataFrame(
        data=[[-0.173, 0.2715, 0.4477, False],
              [-0.135, 0.0784, 0.1287, True],
              [0.0445, 0.0893, 0.1473, False],
              [0.0357, 0.0548, 0.0883, False],
              [0.0123, 0.0384, 0.0643, False],
              [-0.010, 0.0332, 0.0541, False]],
        columns=['effect', 'bootstrap_std',
                 'bootstrap_interval', 'significant_bootstrap'],
        index=['Intercept', 1, 0, 4, 2, 3])
    model.fit(data)

    model.fit_bootstrap(bootstraps=10, n_jobs=1, verbose=False)
    result = model.get_results()

    pd.testing.assert_frame_equal(
        result[expected_result.columns],
        expected_result,
        check_less_precise=1,
        check_index_type=False)

  def test_predict(self):
    data = _prepare_data_and_target()
    model = models.InferenceElasticNet(random_state=18)
    model.fit(data)

    predictions = model.predict(data)

    self.assertIsInstance(predictions, pd.Series)
    self.assertEqual(len(predictions), len(data.data))
    pd.testing.assert_index_equal(predictions.index, data.data.index)

  def test_metrics_calculates_r2_and_mape(self):
    data = _prepare_data_and_target()
    model = models.InferenceElasticNet(random_state=18)
    model.fit(data)

    fit_metrics = ('rmse', 'r2')
    metrics = model.calculate_fit_metrics(data, fit_metrics=fit_metrics)

    self.assertTrue(0 < metrics['r2'] < 1)
    self.assertLess(0, metrics['rmse'])
    self.assertCountEqual(metrics.keys(), fit_metrics)

  def test_permutation_test(self):
    """Ensures the permutation test computes the expected results."""
    data = _prepare_data_and_target()
    model = models.InferenceElasticNet(random_state=18)
    model.fit(data)
    expected_result = pd.DataFrame(
        data=[[-0.20383230, np.nan, np.nan, np.nan, True],
              [-0.13463647, np.nan, np.nan, np.nan, True],
              [0.010821799, np.nan, np.nan, np.nan, True],
              [0.010061159, np.nan, np.nan, np.nan, True],
              [0.000000000, np.nan, np.nan, np.nan, False],
              [0.000000000, np.nan, np.nan, np.nan, False]],
        columns=[
            'effect', 'bootstrap_std', 'bootstrap_interval',
            'significant_bootstrap', 'significant_permutation'],
        index=[1, 'Intercept', 0, 4, 3, 2])

    model.permutation_test(n_permutations=5, verbose=False, n_jobs=1)
    result = model.get_results()

    pd.testing.assert_frame_equal(
        result,
        expected_result,
        check_dtype=False,
        check_index_type=False)

  def test_permutation_test_not_fitted(self):
    """Ensures permutation test can't be run before fitting the model."""
    model = models.InferenceElasticNet(random_state=18)
    data = _prepare_data_and_target()

    with self.assertRaises(RuntimeError):
      model.permutation_test(data)

  def test_fit_with_data_not_ready_throw_error(self):
    data = _prepare_data_and_target(ready_for_modelling=False)
    model = models.InferenceElasticNet(random_state=18)

    with self.assertRaises(data_preparation.InferenceDataError):
      model.fit(data)

  def test_fit_with_data_not_ready_emit_warning(self):
    data = _prepare_data_and_target(ready_for_modelling=False)
    model = models.InferenceLinearRegression()

    with self.assertWarns(data_preparation.InferenceDataWarning):
      model.fit(data, raise_on_data_error=False)

if __name__ == '__main__':
  absltest.main()
