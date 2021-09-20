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

"""Perform bootstrap on LinearModel using multiprocessing.

In linear models, one way to estimate the confidence values for coefficient is
to bootstrap on the data many time and observe how the coefficient varies. In
this module, `regression_bootstrap` offers a simple interface to runs on
many cores when using `sklearn.linear_model.LinearModel` models.
"""

from concurrent import futures
import functools
import gc
import multiprocessing
import sys
import time
from typing import Dict, Text, Optional, Iterable, Any, Iterator, Tuple
import warnings

import numpy as np
import pandas as pd
from sklearn import linear_model

# Models types that are currently supported with cross validation (plus their
# non-cv counterparts)
SUPPORTED_MODELS_CV = (linear_model.RidgeCV, linear_model.LassoLarsCV,
                       linear_model.ElasticNetCV,
                       linear_model.RidgeClassifierCV)

SUPPORTED_MODELS_NO_CV = (linear_model.Ridge, linear_model.LassoLars,
                          linear_model.ElasticNet,
                          linear_model.RidgeClassifier)

# These models support running the cross-validation in parallel
MODELS_THAT_SUPPORT_PARALLEL_CV = (linear_model.RidgeCV,
                                   linear_model.ElasticNetCV,
                                   )


# Default Regressor and RegressorCV
def regressor_default():
  return linear_model.ElasticNet(random_state=30)


def regressor_cv_default():
  return linear_model.ElasticNetCV(
      l1_ratio=[.0001, .1, .5, .7, .9, .95, .99, 1],
      n_alphas=100,
      cv=10,
      random_state=30,
      normalize=True)


def resample(data: pd.DataFrame,
             target: pd.Series,
             sample_frac: float = 1,
             replacement: bool = True,
             min_sample_size: int = 3,
             seed: Optional[int] = None) -> Tuple[pd.DataFrame, pd.Series]:
  """Resamples the data and target provided.

  Args:
    data: DataFrame with features.
    target: Target variable.
    sample_frac: Fraction of data sampled for each bootstrap sample. Defaults to
      1. Allowes oversampling (sample_frac > 1).
    replacement: If True, it allows replacement.
    min_sample_size: Minimum numbers of rows as result of re-samplings.
    seed: Seed to generate the bootstrap sample. If `None`, the seed won't be
      set and results won't be reproducible across multiple runs.

  Returns:
    A tuple with the original dataframe and target variables resampled.

  Raises:
    ValueError if replacement is False and sample_frac > 1, or sample_frac <= 0.
    ValueError if resampling reduces the original data to less than
      `min_sample_size` rows.
  """
  if sample_frac > 1 and not replacement:
    raise ValueError(
        'Cannot take a larger sample than population when "replacement=False"')

  if sample_frac <= 0:
    raise ValueError(f'sample_frac must be > 0, not {sample_frac}.')
  sample_size = int(sample_frac * len(data))

  if sample_size < min_sample_size:
    raise ValueError(
        f'sample_frac*len(data) must be > {min_sample_size}, not {sample_size}.'
        f' Note: int(sample_frac * len(data)) must be >= {min_sample_size}')

  if seed is not None:
    np.random.seed(seed)

  indices = np.random.choice(len(data), sample_size, replace=replacement)
  return data.iloc[indices], target.iloc[indices]


def regression_iterate(regressor: linear_model._base.LinearModel,
                       data: pd.DataFrame,
                       target: pd.Series,
                       sample_frac: float = 1,
                       replacement: bool = True,
                       randomize_target: bool = False,
                       seed: Optional[int] = None) -> Dict[Text, float]:
  """Runs a bootstrap or permutation iteration and returns linear coefficients.

  Args:
    regressor: Regressor.
    data: DataFrame with features.
    target: Target variable.
    sample_frac: Fraction of data sampled for each bootstrap sample. Defaults to
      1. Allowes oversampling (sample_frac > 1).
    replacement: If True, it allows replacement.
    randomize_target: If True, this will randomly shuffle the target Series when
      used for permutation test.
    seed: Seed to generate the bootstrap sample. If `None`, the seed won't be
      set and results won't be reproducible across multiple runs.

  Returns:
    Dictionary with the feature name as Key and the coefficient as Value.
  """
  if randomize_target:
    # Only used when running the permutation test.
    target = target.sample(frac=1, replace=False, random_state=seed)
  else:
    # Running the bootstrap.
    data, target = resample(
        data, target, sample_frac, replacement, seed=seed)

  regressor.fit(data, target)

  columns = list(data.columns)
  if regressor.get_params()['fit_intercept']:
    columns.append('Intercept')
    coef = np.append(regressor.coef_, regressor.intercept_)
  else:
    coef = regressor.coef_
  return dict(zip(columns, coef))


def permutation_test(
    data: pd.DataFrame,
    target: pd.Series,
    regressor: linear_model._base.LinearModel = regressor_default(),
    n_permutations: int = 100,
    n_jobs: int = 1,
    verbose: bool = True) -> pd.DataFrame:
  """Runs permutation test on a linear regressor.

  A permutation test is also called a randomization test, re-randomization test
  or an exact test. More information here:
  https://en.wikipedia.org/wiki/Resampling_(statistics)#Permutation_tests_2

  Args:
    data: DataFrame with features.
    target: Target variable.
    regressor: Regressor to be used in the permutation test.
    n_permutations: Number of permutations to perform.
    n_jobs: If use multiple CPUs during the bootstrap. Default value is `1` for
      no multiprocessing. You can specify the number of CPUs or specify negative
      values will all CPUs available minus the value selected. For example: `-1`
        will use all CPUs but one.
    verbose: If True, will print to screen CV results and estimated completion
      time for bootstrap.

  Returns:
    A DataFrame of coefficients with a row for each permutation and one column
    for each feature. Plus the intercept if the model has intercept.
  """
  return regression_bootstrap(
      data=data,
      target=target,
      regressor=regressor,
      regressor_cv=None,
      bootstraps=n_permutations,
      randomize_target=True,
      n_jobs=n_jobs,
      verbose=verbose)


def regression_bootstrap(
    data: pd.DataFrame,
    target: pd.Series,
    regressor: linear_model._base.LinearModel = regressor_default(),
    regressor_cv: Optional[linear_model._base.LinearModel] = \
    regressor_cv_default(),
    bootstraps: Optional[int] = 1000,
    sample_frac: float = 1,
    replacement: bool = True,
    randomize_target: bool = False,
    n_jobs: Optional[int] = 1,
    verbose: Optional[bool] = True) -> pd.DataFrame:
  """Runs bootstrapping and tunes hyperparameters on a linear regressor.

  Args:
    data: DataFrame with features.
    target: Target variable.
    regressor: Regressor to be used in the bootstrap.
    regressor_cv: Regressor with cross-validation with sklearn implementation.
      If set to `None`, the initial parameter tuning will be skipped.
    bootstraps: n=Number of bootstraps to perform.
    sample_frac: Fraction of data sampled for each bootstrap sample. Defaults to
      1. Allowes oversampling (sample_frac > 1).
    replacement: If True, it allows replacement.
    randomize_target: If True, this will randomly shuffle the target Series when
      used for permutation test.
    n_jobs: If use multiple CPUs during the bootstrap. Default value is `1` for
      no multiprocessing. You can specify the number of CPUs or specify negative
      values will all CPUs available minus the value selected. For example: `-1`
        will use all CPUs but one.
    verbose: If True, will print to screen CV results and estimated completion
      time for bootstrap.

  Returns:
    A DataFrame of coefficients with a row for each bootstrap and one column for
    each feature. Plus the intercept if the model has intercept.

  Raises:
    RuntimeError: if `regressor` is a CV type model in {'RidgeCV', 'LarsCV',
    'LassoLarsCV', 'ElasticNetCV', 'RidgeClassifierCV'} and regressor_cv is not
    None.
    TypeError: if a mismatch between regressor and regressor_cv is supplied.
  """
  if isinstance(regressor, SUPPORTED_MODELS_CV) and regressor_cv is not None:
    raise RuntimeError(
        'regressor_cv must be None if regressor is a CV type in'
        f'[{", ".join([str(model) for model in SUPPORTED_MODELS_CV])}]')

  if regressor_cv is not None:
    # tune hyperparams once before we start bootstrapping.
    cv_params = _tune_hyperparams(data, target, regressor_cv, n_jobs)
    regressor.set_params(**cv_params)
    if verbose:
      print(cv_params)

  # Perform boostrapping
  t_init = time.time()
  regr_iterate_partial = functools.partial(
      regression_iterate, regressor, data, target,
      sample_frac, replacement, randomize_target)
  cpu_count = multiprocessing.cpu_count()  # pytype: disable=unsupported-operands
  n_jobs = max(1, min(cpu_count + n_jobs if n_jobs < 0 else n_jobs, cpu_count))  # pytype: disable=unsupported-operands

  if n_jobs == 1:
    iterator = map(regr_iterate_partial, range(bootstraps))
    bootstrap_list = list(
        _yield_bootstraps_print_eta(iterator, t_init, n_jobs, bootstraps,
                                    verbose))
    return pd.DataFrame(bootstrap_list)
  else:
    try:
      with futures.ProcessPoolExecutor(n_jobs) as pool:
        iterator = pool.map(regr_iterate_partial, range(bootstraps))
        bootstrap_list = list(
            _yield_bootstraps_print_eta(iterator, t_init, n_jobs, bootstraps,
                                        verbose))
      # Calling manually garbage collector below to try avoiding triggering an
      # OSError("handle is closed") until gps-building-blocks will support
      # python 3.9+.
      # https://stackoverflow.com/questions/63926326/python-concurrent-futures-error-in-atexit-run-exitfuncs-oserror-handle-is-clo
      gc.collect()
      if len(bootstrap_list) != bootstraps:
        warnings.warn(
            f'Returning {len(bootstrap_list)} bootstraps instead of {bootstraps}'
        )
      return pd.DataFrame(bootstrap_list)

    except AttributeError:
      warn_msg = ('Couldn\'t run multiprocessing using\n', 'Process Pool.')
      warnings.warn(warn_msg)  # pytype: disable=wrong-arg-types
      return pd.DataFrame([])
    finally:
      # Calling manually garbage collector below to try avoiding triggering an
      # OSError("handle is closed") until gps-building-blocks will support
      # python 3.9+.
      # https://stackoverflow.com/questions/63926326/python-concurrent-futures-error-in-atexit-run-exitfuncs-oserror-handle-is-clo
      gc.collect()


def _tune_hyperparams(
    data: pd.DataFrame,
    target: pd.Series,
    regressor_cv: linear_model._base.LinearModel,
    n_jobs: Optional[int] = 1) -> Dict[Text, float]:
  """Tunes the hyperparameters prior to bootstraping.

  Args:
    data: DataFrame with features.
    target: Target variable.
    regressor_cv: Regressor with cross-validation with sklearn implementation.
    n_jobs: passed to regressor_cv.set_param if it accepts n_jobs.

  Returns:
    A dict of optimal hyperparamters.

  Raises:
    NotImplementedError: if `regressor_cv` is not supported. Currently
    supporting {'RidgeCV', 'LarsCV', 'LassoLarsCV', 'ElasticNetCV',
    'RidgeClassifierCV'}.
  """
  if not isinstance(regressor_cv, SUPPORTED_MODELS_CV):
    raise NotImplementedError(
        f'CV not supported for "{type(regressor_cv)}". Model currently '
        f'supported are [{", ".join([str(model) for model in SUPPORTED_MODELS_CV])}]'
    )

  # Set multiproccessing if regressor_cv.set_params supports n_jobs.
  if 'n_jobs' in regressor_cv.get_params():
    if isinstance(regressor_cv, MODELS_THAT_SUPPORT_PARALLEL_CV):
      regressor_cv.set_params(n_jobs=n_jobs)
    else:
      regressor_cv.set_params(n_jobs=1)

  regressor_cv.fit(data, target)

  if isinstance(regressor_cv, linear_model.ElasticNetCV):
    # If it's ElasticNetCV and we need to take also the `l1_ratio`.
    cv_params = {
        'l1_ratio': regressor_cv.l1_ratio_,
        'alpha': max(regressor_cv.alpha_, 0.0001)
    }
  else:
    # otherwise all models so far only have the alpha hyperparameter.
    cv_params = {'alpha': regressor_cv.alpha_}

  return cv_params


def _print_bootstrap_eta(i: int, t_init: float, n_jobs: int,
                         bootstraps: int) -> None:
  """Calculates ETA and prints timing stats."""
  elapsed = time.time() - t_init
  avg_cpu_time = elapsed / (i + 1) * n_jobs
  estimated = avg_cpu_time * np.ceil(bootstraps / n_jobs)
  remaining = estimated - elapsed

  print(f'avg cpu time per fit: {avg_cpu_time:.2f}s | '
        f'elapsed: {elapsed / 60:.2f}m | '
        f'estimated: {estimated / 60:.2f}m | '
        f'remaining: {remaining / 60:.2f}m')
  # Making sure this will be printed as sometime it is buffered
  sys.stdout.flush()


def _yield_bootstraps_print_eta(iterator: Iterable[Any], t_init: float,
                                n_jobs: int, bootstraps: int,
                                verbose: bool) -> Iterator[Any]:
  """Loop over iterator and yield results. Print eta if verbose."""
  # Keep track of completion time
  for i, coefficient in enumerate(iterator):
    yield coefficient
    # Print to console ETA only if verbose and every 'five' fit
    if verbose and (i % 5 == 0):
      _print_bootstrap_eta(i, t_init, n_jobs, bootstraps)
