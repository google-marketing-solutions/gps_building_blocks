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

"""Module containing the InferenceModel(s) classes.

InferenceModels are "wrappers" of existing implementation of models, they
provides additional information specifically for inference analysis. Such as
the effect for each feature and if statistically significant. For example,
InferenceElasticNet is a wrapper of ElasticNet in `sklearn.linear_model`.

  Typical usage example:

  # Your experiment data
  some_data = pd.DataFrame(
      data=[[0.0, 1.0, 10.0],
            [0.0, 1.0, 10.0],
            [1.0, 1.0, 5.00],
            [1.0, 0.0, 0.00]],
      columns=['control', 'variable', 'outcome'])
  data = inference.InferenceData(
      initial_data=some_data,
      target_column='outcome')
  data.fixed_effect(['control'], strategy='quick', min_frequency=2)

  model = InferenceElasticNet()
  model.fit(data)

  model.get_results(confidence_level=0.95)
"""

import abc
from typing import Any, MutableMapping, Text

import numpy as np
import pandas as pd
import scipy.stats
from sklearn import linear_model

from gps_building_blocks.ml.diagnostics import bootstrap
from gps_building_blocks.ml.statistical_inference import data_preparation


class InferenceModel(metaclass=abc.ABCMeta):
  """Superclass for all InferenceModel objects within `statistical_inference`.

  Attributes:
    model: The underlying model for each implementation. For linear_model in
      scikit-learn, these can be any of LinearRegression, ElasticNet. Users can
      also implements non scikit-learn models as long as they are implementing
      all of the following abstract methods.

  Each child InferenceModel class, should implement the following:

    `fit` to train the model accordingly.
    `_extract_effects` to retrieve the effect for each feature.
    `_extract_confidence_intervals` to retrieve the confidence intervals for
      each feature.
  """

  def __init__(self, model: Any) -> None:
    self.model = model
    self._is_fit = False
    self._bootstrap_results = None
    self._permutation_results = None
    self._data = None

  @abc.abstractmethod
  def _extract_effects(self) -> MutableMapping[Text, float]:
    """Returns effect for each feature in the model."""

  def _extract_confidence_intervals(
      self, confidence_level: float = 0.95) -> MutableMapping[Text, float]:
    """Returns the confidence interval around the effect for each feature."""
    if isinstance(self._bootstrap_results, pd.DataFrame):
      significance_level = scipy.stats.norm.ppf(confidence_level)
      return (significance_level * self._bootstrap_results.std()).to_dict()
    else:
      return {}

  def _extract_boostrap_std(
      self) -> MutableMapping[Text, float]:
    """Returns the bootstrap standard deviation for each feature."""
    if isinstance(self._bootstrap_results, pd.DataFrame):
      return self._bootstrap_results.std().to_dict()
    else:
      return {}

  def _calculate_permutation_test_results(
      self, effect: pd.Series, significance_level: float = 0.05
      ) -> MutableMapping[Text, float]:
    """Calculates the permutation test results if available."""
    if isinstance(self._permutation_results, pd.DataFrame):
      lower_percentile = significance_level / 2
      upper_percentile = 1 - lower_percentile
      # Verify if effect is in left tail of the distribution.
      significant = effect.gt(
          self._permutation_results.quantile(upper_percentile))
      # Verify if effect is in right tail of the distribution.
      significant |= effect.lt(
          self._permutation_results.quantile(lower_percentile))
      return significant.to_dict()
    else:
      return {}

  def get_results(self, confidence_level: float = 0.95) -> pd.DataFrame:
    """Returns the effect and statistical information for each feature.

    If available, this will return if the feature is statistical significant
    with the chosen `confidence_level` and it's confidence interval.

    Args:
      confidence_level: Probability for the plausible values to be within the
        interval for each feature.

    Returns:
      A DataFrame with the following information for each features. `effect`,
      `confidence_interval` and `significant` if statistically significant.
      Results will be sorted in descending order by effect magnitude.

    Raises:
      RuntimeError: if the Model has not being fit before calling this method.
    """
    if not self._is_fit:
      raise RuntimeError(
          'InferenceModel must be fit before requesting results.')

    effects = self._extract_effects()
    confidence_intervals = self._extract_confidence_intervals(confidence_level)

    results = pd.Series(effects).rename('effect').to_frame()
    results['bootstrap_std'] = pd.Series(
        self._extract_boostrap_std(), dtype='float64')
    results['confidence_interval'] = pd.Series(
        confidence_intervals, dtype='float64')
    results['significant_bootstrap'] = np.nan
    if confidence_intervals:
      # populate only if bootstrap intervals are available
      results['significant_bootstrap'] = (
          results['effect'].abs() > results['confidence_interval'])

    permutation_results = self._calculate_permutation_test_results(
        results['effect'], 1 - confidence_level)
    results['significant_permutation'] = pd.Series(
        permutation_results, dtype=bool)

    # Sort by `effect` magnitude
    results = results.iloc[results['effect'].abs().argsort()[::-1]]

    return results

  def fit(
      self,
      data: data_preparation.InferenceData,
      raise_on_data_error: bool = True,
      **kwargs) -> None:
    """Fits the model with the data and target provided.

    Args:
      data: InferenceData object with the data and target for you analysis.
      raise_on_data_error: Weather to raise an exception if a problem if found
        with any data `checks` on the provided InferenceData. If set to False,
        the integrity checks may emit InferenceDataWarning warnings.
      **kwargs: Any additional parameter to sent to `underlying` model.

    Raises:
      MissingValueError: If the latest transformation of the data has columns
        with missing values.
      ControlVariableError: If the latest transformation of the data hasn't gone
        through a method to control for external factors.
      LowVarianceError: If the latest transformation of the data hasn't gone
        through a method to address feature with low variance.
      CollinearityError: If the latest transformation of the data hasn't gone
        through a method to address collinearity.
    """
    data.data_check(raise_on_data_error)

    modelling_data, target = data.get_data_and_target()
    self._fit(modelling_data, target, **kwargs)
    self._data = data
    self._is_fit = True

  @abc.abstractmethod
  def _fit(self, data: pd.DataFrame, target: pd.Series, **kwargs) -> None:
    """Wrapper around native fit methods to enables a single standard format."""

  def predict(
      self, data: data_preparation.InferenceData, **kwargs) -> pd.Series:
    """Predicts target variable with the data provided.

    Args:
      data: InferenceData object with the data you want to score.
      **kwargs: Any additional parameter to sent to `underlying` model.

    Returns:
      Series with the predictions.

    Raises:
      RuntimeError: if the Model has not being fit before calling this method.
    """
    if not self._is_fit:
      raise RuntimeError(
          'InferenceModel must be fit before making predictions.')

    modelling_data, _ = data.get_data_and_target()
    return pd.Series(
        data=self._predict(modelling_data, **kwargs),
        index=modelling_data.index)

  @abc.abstractmethod
  def _predict(self, data: pd.DataFrame, **kwargs) -> pd.Series:
    """Predicts using the underlying model implementation."""


class InferenceLinearRegressionModel(InferenceModel, metaclass=abc.ABCMeta):
  """Parent class for all `Scikit-Learn` linear models.

  This class is not meant to be user directly.
  """

  def _extract_effects(self) -> MutableMapping[Text, float]:
    """Returns effect for each feature in the model.

    This will return the coefficient value for each feature. If the model has
    been fitted with bootstrap, it will return the average of the coefficients
    for each features among all the boostrapped fits.
    """
    if isinstance(self._bootstrap_results, pd.DataFrame):
      return self._bootstrap_results.mean().to_dict()
    else:
      feature_names = self._data.data.columns
      coefficients = dict(zip(feature_names, self.model.coef_))
      if self.model.fit_intercept:
        coefficients['Intercept'] = self.model.intercept_
      return coefficients

  def _fit(self, data: pd.DataFrame, target: pd.Series, **kwargs) -> None:
    """Fits the underlying `Scikit-Learn` linear model.

    Args:
      data: DataFrame with the input features for the model.
      target: Target variable to regress to.
      **kwargs: Any additional parameters to send to the fit function.
    """
    self.model.fit(data, target, **kwargs)

  def fit_bootstrap(
      self,
      bootstraps: int = 1000,
      n_jobs: int = -1,
      verbose: bool = True,
      ) -> None:
    """Runs a bootstrap iteration to estimate confidence intervals.

    Args:
      bootstraps: Number of bootstraps to perform.
      n_jobs: If use multiple CPUs during the bootstrap. Default value is `1`
        for no multiprocessing. You can specify the number of CPUs or specify
        negative values will all CPUs available minus the value selected.
      verbose: If True, will print to screen CV results and estimated completion
        time for bootstrap.

    Raises:
      RuntimeError: if the InferenceLinearRegressionModel has not being fit
        before calling this method.
    """
    if not self._is_fit:
      raise RuntimeError(
          'InferenceModel must be fit before running bootstrap.')

    modelling_data, target = self._data.get_data_and_target()

    # Backup coefficients and intercept.
    coefficients = self.model.coef_.copy()
    intercept = self.model.intercept_.copy()

    try:
      self._bootstrap_results = bootstrap.regression_bootstrap(
          data=modelling_data,
          target=target,
          regressor=self.model,
          regressor_cv=None,
          bootstraps=bootstraps,
          n_jobs=n_jobs,
          verbose=verbose)
    finally:
      # Restore coefficients and intercept.
      self.model.coef_ = coefficients
      self.model.intercept_ = intercept

  def _predict(self, data: pd.DataFrame, **kwargs) -> pd.Series:
    """Predicts using the underlying model implementation."""
    return self.model.predict(data)

  def permutation_test(
      self,
      n_permutations: int = 100,
      n_jobs: int = -1,
      verbose: bool = True) -> None:
    """Runs a permutation test.

    Args:
      n_permutations: Number of permutations to perform.
      n_jobs: If use multiple CPUs during the bootstrap. Default value is `1`
        for no multiprocessing. You can specify the number of CPUs or specify
        negative values will all CPUs available minus the value selected.
      verbose: If True, will print to screen CV results and estimated completion
        time for bootstrap.

    Raises:
      RuntimeError: if the InferenceLinearRegressionModel has not being fit
        before calling this method.
    """
    if not self._is_fit:
      raise RuntimeError(
          'InferenceModel must be fit before running permutation test.')

    modelling_data, target = self._data.get_data_and_target()

    # Backup coefficients and intercept.
    coefficients = self.model.coef_.copy()
    intercept = self.model.intercept_.copy()

    try:
      self._permutation_results = bootstrap.permutation_test(
          data=modelling_data,
          target=target,
          regressor=self.model,
          n_permutations=n_permutations,
          n_jobs=n_jobs,
          verbose=verbose)
    finally:
      # Restore coefficients and intercept.
      self.model.coef_ = coefficients
      self.model.intercept_ = intercept


class InferenceElasticNet(InferenceLinearRegressionModel):
  """Linear regression with combined L1 and L2 priors as regularizer.

  You can customize the model passing any existing parameters of the original
  ElasticNet in `Scikit-Learn` implementation. More information on the whole set
  of parameters and user guide can be found in the scikit-learn documentation at
  https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.ElasticNet.html#sklearn.linear_model.ElasticNet

  The parameter `l1_ratio` corresponds to `alpha` in the glmnet R package
  while `alpha` corresponds to the `lambda` parameter in glmnet. Specifically,
  `l1_ratio = 1` is the lasso penalty. Currently, `l1_ratio <= 0.01` is not
  reliable, unless you supply your own sequence of alpha."
  """

  def __init__(self, **kwargs) -> None:
    model = linear_model.ElasticNet(**kwargs)
    super().__init__(model)


class InferenceElasticNetCV(InferenceLinearRegressionModel):
  """Elastic Net model with iterative fitting along a regularization path.

  You can customize the model passing any existing parameters of the original
  ElasticNetCV in `Scikit-Learn` implementation. More information on the whole
  set of parameters and user guide can be found in the scikit-learn
  documentation at
  https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.ElasticNetCV.html#sklearn.linear_model.ElasticNetCV
  """

  def __init__(self, **kwargs) -> None:
    model = linear_model.ElasticNetCV(**kwargs)
    super().__init__(model)
