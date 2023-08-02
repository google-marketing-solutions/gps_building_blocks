# Licensed under the Apache License, Version 2.0
"""Tests for gps_building_blocks.py.ml.diagnostics.bootstrap."""

import numpy as np
import pandas as pd
import pandas.testing as pandas_testing
from sklearn import linear_model

from absl.testing import absltest
from absl.testing import parameterized
from gps_building_blocks.ml.diagnostics import bootstrap


class BootstrapTest(parameterized.TestCase):

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    # Prepare data
    np.random.seed(42)
    n_samples, n_features = 35, 70
    data = np.random.randn(n_samples, n_features)
    # Decreasing coefficients w. alternated signs for visualization
    idx = np.arange(n_features)
    coefficients = (-1) ** idx * np.exp(-idx / 10)
    coefficients[10:] = 0  # sparsify
    target = np.dot(data, coefficients)
    # Add noise
    target += 0.01 * np.random.normal(size=n_samples)
    classification_target = np.where(target > np.median(target), 1, 0)
    cls.data = pd.DataFrame(data)
    cls.target = pd.Series(target)
    cls.class_target = pd.Series(classification_target)

  def test_regression_iterate(self):
    elastic_net = linear_model.ElasticNet()
    expected_keys = set(list(self.data.columns) + ['Intercept'])

    result = bootstrap.regression_iterate(
        elastic_net, self.data, self.target, seed=1)

    self.assertIsInstance(result, dict)
    # Assert all the keys are the DataFrame columns plus the intercept.
    self.assertEqual(expected_keys, set(result))

  @parameterized.named_parameters(
      ('undersample', 0.5),
      ('oversample', 2))
  def test_regression_bootstrap_sample_frac(self, sample_frac):
    linear = linear_model.LinearRegression()

    result = bootstrap.regression_bootstrap(
        self.data, self.target, linear,
        regressor_cv=None,
        bootstraps=5)
    coef_std = result.std(axis=0).mean()

    result_sampled = bootstrap.regression_bootstrap(
        self.data, self.target, linear,
        regressor_cv=None,
        bootstraps=5,
        sample_frac=sample_frac)
    coef_std_sampled = result_sampled.std(axis=0).mean()

    self.assertNotEqual(coef_std, coef_std_sampled)

  @parameterized.named_parameters(
      ('negative', -0.5),
      ('less_than_degfreedom', 1/35))
  def test_regression_bootstrap_sample_frac_valueerror(self, sample_frac):
    linear = linear_model.LinearRegression()
    with self.assertRaises(ValueError):
      bootstrap.regression_bootstrap(
          self.data, self.target, linear,
          regressor_cv=None,
          bootstraps=5,
          sample_frac=sample_frac)

  def test_regression_iterate_no_intercept(self):
    elastic_net = linear_model.ElasticNet(fit_intercept=False)
    expected_keys = set(self.data.columns)

    result = bootstrap.regression_iterate(
        elastic_net, self.data, self.target, seed=1)

    self.assertIsInstance(result, dict)
    # Assert all the keys are the DataFrame columns.
    self.assertEqual(expected_keys, set(result))

  def test_regression_iterate_seed(self):
    elastic_net = linear_model.ElasticNet(random_state=123)
    expected_result = bootstrap.regression_iterate(
        elastic_net, self.data, self.target, seed=1)

    result = bootstrap.regression_iterate(
        elastic_net, self.data, self.target, seed=1)

    self.assertIsInstance(result, dict)
    self.assertEqual(result, expected_result)

  @parameterized.named_parameters(
      ('default_regressor_default_regressor_cv', bootstrap.regressor_default(),
       bootstrap.regressor_cv_default().set_params(cv=3, n_alphas=10), 5, 1),
      ('default_regressor_none_regressor_cv', bootstrap.regressor_default(),
       None, 5, 1),
      ('linear_regressor_none_regressor_cv', linear_model.LinearRegression(),
       None, 5, 1),
      ('inner_regressor_cv',
       bootstrap.regressor_cv_default().set_params(cv=3, n_alphas=10), None, 5,
       1),
      ('elastic_net_cv_multiproc', linear_model.ElasticNet(),
       bootstrap.regressor_cv_default().set_params(cv=3, n_alphas=10), 5, -1),
      ('elastic_net_cv_multiproc_4cpus', linear_model.ElasticNet(),
       linear_model.ElasticNetCV(cv=3), 5, 4),
      ('elastic_net_cv', linear_model.ElasticNet(),
       linear_model.ElasticNetCV(cv=3), 5, 1),
      ('elastic_net_multiproc', linear_model.ElasticNet(), None, 5, -1),
      ('ridge', linear_model.Ridge(), None, 5, 1),
      ('ridge_multiproc', linear_model.Ridge(), None, 5, -1),
      ('ridge_multiproc_10_bootstraps', linear_model.Ridge(), None, 10, -1),
      ('lasso', linear_model.Lasso(), None, 5, 1),
      ('lasso_multiproc', linear_model.Lasso(), None, 5, -1),
      ('lars', linear_model.Lars(n_nonzero_coefs=5), None, 5, 1),
      ('lars_multiproc', linear_model.Lars(n_nonzero_coefs=5), None, 5, -1))
  def test_regression_bootstrap(
      self, regressor, regressor_cv, bootstraps, n_jobs):
    result = bootstrap.regression_bootstrap(
        data=self.data,
        target=self.target,
        regressor=regressor,
        regressor_cv=regressor_cv,
        bootstraps=bootstraps,
        n_jobs=n_jobs,
        verbose=False)

    self.assertIsInstance(result, pd.DataFrame)
    self.assertLen(result, bootstraps)  # Same rows as many bootstraps
    self.assertEqual(result.shape[1], self.data.shape[1]+1)

  def test_classification_bootstrap(self):
    ridge_class = linear_model.RidgeClassifier()
    ridge_class_cv = linear_model.RidgeClassifierCV()

    result = bootstrap.regression_bootstrap(
        data=self.data,
        target=self.class_target,
        regressor=ridge_class,
        regressor_cv=ridge_class_cv,
        verbose=False,
        bootstraps=5)

    self.assertIsInstance(result, pd.DataFrame)
    self.assertEqual(result.shape[1], self.data.shape[1]+1)

  @parameterized.named_parameters(
      ('linear_regressor_elastic_net_regressor_cv',
       linear_model.LinearRegression(), bootstrap.regressor_cv_default(), 5, 1),
      ('ridge_regressor_elastic_net_regressor_cv',
       linear_model.Ridge(), bootstrap.regressor_cv_default(), 5, 1),
      ('lassolars_regressor_elastic_net_regressor_cv',
       linear_model.LassoLars(), bootstrap.regressor_cv_default(), 5, 1))
  def test_regression_bootstrap_mismatch_regressor_cv(
      self, regressor, regressor_cv, bootstraps, n_jobs):
    with self.assertRaises(ValueError):
      bootstrap.regression_bootstrap(
          data=self.data,
          target=self.target,
          regressor=regressor,
          regressor_cv=regressor_cv,
          bootstraps=bootstraps,
          n_jobs=n_jobs,
          verbose=False)

  @parameterized.named_parameters(
      ('elasticnet_elasticnet', linear_model.ElasticNet(),
       linear_model.ElasticNet()),
      ('none_elasticnet', None, linear_model.ElasticNet()))
  def test_regression_bootstrap_unsupported_regressor_cv(
      self, regressor, regressor_cv):
    with self.assertRaises(NotImplementedError):
      bootstrap.regression_bootstrap(
          self.data,
          self.target,
          regressor=regressor,
          regressor_cv=regressor_cv)

  @parameterized.named_parameters(
      ('elasticnetcv_elasticnetcv', linear_model.ElasticNetCV(),
       linear_model.ElasticNetCV()),
      ('ridgecv_ridgecv', linear_model.RidgeCV(), linear_model.RidgeCV()),
      ('ridgecv_elasticnetcv', linear_model.RidgeCV(),
       linear_model.ElasticNetCV()))
  def test_regression_bootstrap_runtime_error(
      self, regressor, regressor_cv):
    with self.assertRaises(RuntimeError):
      bootstrap.regression_bootstrap(
          self.data,
          self.target,
          regressor=regressor,
          regressor_cv=regressor_cv)

  def test_regression_not_numeric_index(self):
    """Makes sure that regression_iterate handles non numeric indexing."""
    elastic_net = linear_model.ElasticNet(random_state=123)
    data = self.data.copy()
    target = self.target.copy()
    # Convert index to string for `data` and `target`
    data.index = [f'data_{index}' for index in data.index]
    target.index = [f'test_{index}' for index in target.index]
    expected_result = bootstrap.regression_iterate(
        elastic_net, self.data, self.target, seed=1)

    result = bootstrap.regression_iterate(
        elastic_net, data, target, seed=1)

    self.assertIsInstance(result, dict)
    self.assertEqual(result, expected_result)


  def test_regression_bootstrap_sampled_hyperpar_tune(self):
    """Compares the single and multi hyperparameter tuning."""
    # Single hyperparameter tune prior to bootstrapping.
    kwargs = {'data': self.data,
              'target': self.target,
              'bootstraps': 5}

    elastic_net = linear_model.ElasticNet(random_state=1)
    elastic_net_cv = linear_model.ElasticNetCV(random_state=10, cv=3)
    outer_tune = bootstrap.regression_bootstrap(
        regressor=elastic_net, regressor_cv=elastic_net_cv, **kwargs)
    outer_coef_std = outer_tune.std(axis=0).mean()

    # Hyperparameters re-tuned on every bootstrap sample.
    elastic_net = linear_model.ElasticNetCV(random_state=10, cv=3)
    elastic_net_cv = None
    outer_inner_tune = bootstrap.regression_bootstrap(
        regressor=elastic_net, regressor_cv=elastic_net_cv, **kwargs)
    outer_inner_coef_std = outer_inner_tune.std(axis=0).mean()

    # Confirm that running separate instances gives same results for single
    # tune. This is identical setup to outer_tune.
    elastic_net = linear_model.ElasticNet(random_state=1)
    elastic_net_cv = linear_model.ElasticNetCV(random_state=10, cv=3)
    outer_tune2 = bootstrap.regression_bootstrap(
        regressor=elastic_net, regressor_cv=elastic_net_cv, **kwargs)
    outer2_coef_std = outer_tune2.std(axis=0).mean()

    self.assertNotEqual(outer_coef_std, outer_inner_coef_std)
    self.assertEqual(outer_coef_std, outer2_coef_std)

  def test_resample_without_replacement(self):
    """Ensures sampling without replacement is working as intended."""
    resampled_data, resampled_target = bootstrap.resample(
        self.data, self.target, replacement=False)

    # Make sure there are no duplicate rows. `nunique` returns the numbers of
    # unique rows in a series.
    self.assertEqual(resampled_data.index.nunique(), len(self.data))
    # Make sure data and target index match
    self.assertListEqual(
        resampled_data.index.tolist(), resampled_target.index.tolist())

  def test_resample_with_replacement(self):
    """Ensures sampling with replacement is working as intended."""
    resampled_data, _ = bootstrap.resample(
        self.data, self.target, replacement=True)

    # Make sure there are duplicate rows as we're using replacement
    self.assertLess(resampled_data.index.nunique(), len(self.data))

  def test_resample_replacement_oversample(self):
    """Ensures oversampling without replacement is not allowed."""
    with self.assertRaises(ValueError):
      _ = bootstrap.resample(
          self.data, self.target, sample_frac=2, replacement=False)

  def test_regression_iterate_randomize_target(self):
    """Ensures the target randomization delivers different results."""
    kw = {'regressor': linear_model.Ridge(),
          'data': self.data,
          'target': self.target}

    bootstrapped_results = bootstrap.regression_iterate(
        randomize_target=False, **kw)
    randomized_results = bootstrap.regression_iterate(
        randomize_target=True, **kw)

    # Checks randomize and bootstrap results have same keys.
    self.assertEqual(randomized_results.keys(), bootstrapped_results.keys())
    # But they have different data.
    self.assertNotEqual(
        randomized_results.values(), bootstrapped_results.values())

  def test_regression_bootstrap_without_replacement(self):
    """Compares results with and without replacement."""
    kwargs = {'data': self.data,
              'target': self.target,
              'regressor': linear_model.Ridge(),
              'regressor_cv': None,
              'sample_frac': 0.8,
              'bootstraps': 5}
    replacement_result = bootstrap.regression_bootstrap(
        replacement=True, **kwargs)

    without_replacement_result = bootstrap.regression_bootstrap(
        replacement=False, **kwargs)

    # Results should be different as data has been resampled differently.
    self.assertFalse(replacement_result.equals(without_replacement_result))

  @parameterized.named_parameters(
      ('with_intercept', linear_model.Ridge(fit_intercept=True), 5),
      ('without_intercept', linear_model.Ridge(fit_intercept=False), 5))
  def test_permutation_test(self, regressor, n_permutations):
    """Ensure the permutation test works as intended."""
    feature_names = self.data.columns.tolist()
    if regressor.fit_intercept:
      feature_names.append('Intercept')

    permutation_results = bootstrap.permutation_test(
        data=self.data, target=self.target, regressor=regressor,
        n_permutations=n_permutations, n_jobs=1, verbose=False)

    self.assertLen(permutation_results, n_permutations)
    self.assertListEqual(permutation_results.columns.tolist(), feature_names)

  def test_permutation_test_seed(self):
    """Ensures the permutation results are reproducible and seed works."""
    kw = {
        'data': self.data, 'target': self.target,
        'regressor': linear_model.Ridge(), 'n_permutations': 3,
        'n_jobs': 1, 'verbose': False}

    first_results = bootstrap.permutation_test(**kw)
    second_results = bootstrap.permutation_test(**kw)

    pandas_testing.assert_frame_equal(first_results, second_results)


if __name__ == '__main__':
  absltest.main()
