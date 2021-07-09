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
"""Functions commonly used to perform feature enineering."""

from typing import List, Optional
import warnings

import numpy as np
import pandas as pd
from statsmodels.stats.outliers_influence import variance_inflation_factor
from statsmodels.tools.tools import add_constant


class IllConditionedDataWarning(Warning):
  pass


class IllConditionedDataError(Exception):
  pass


class SingularDataError(Exception):
  pass


def _calculate_vif_using_correlation_matrix_inversion(
    data: pd.DataFrame,
    raise_on_ill_conditioned: bool = False,
    corr_matrix: Optional[pd.DataFrame] = None) -> List[float]:
  """Calculates VIF from the diagonals of the inverted correlation matrix.

  Args:
    data: Must not include the response variable (Y). Must be numeric (no
      strings or categories).
    raise_on_ill_conditioned: Whether to raise an exception if the correlation
      matrix is ill-conditioned (if True), or just throw a warning (if False).
    corr_matrix: Computing the correlation matrix of the input data is somewhat
      expensive, so if the user has already computed it outside this function,
      pass it here so it will not have to be re-computed.

  Returns:
    vifs: List of VIFs for each feature (column) in the input data. The VIFs in
      the list are returned in the same order as the columns of the input data.

  Raises:
    IllConditionedDataWarning: if the input data have too high a condition
      number the VIFs can be unreliable.
    IllConditionedDataError: if the input data have too high a condition
      number the VIFs can be unreliable.
    SingularDataError: if the correlation matrix is singular, this method
      cannot compute the VIFs so this function will fail and raise this
      exception.
  """
  if corr_matrix is None:
    corr_matrix = data.astype(float).corr()
  identity_matrix = np.identity(len(data.columns), dtype=float)
  try:
    vifs = np.linalg.solve(corr_matrix, identity_matrix).diagonal().tolist()
    if np.linalg.cond(corr_matrix, p=np.inf) > 0.1 / np.finfo(float).eps:
      message = (
          'The correlation matrix has a high condition number. Recommend '
          'checking the input data for nearly constant or nearly identical '
          'columns, and/or dropping the columns with highest VIF.')
      if raise_on_ill_conditioned:
        raise IllConditionedDataError(message)
      else:
        warnings.warn(IllConditionedDataWarning(message))
  except np.linalg.LinAlgError:
    message = (
        'The correlation matrix is singular. Recommend checking the input data'
        ' for constant, identical, or perfectly multicollinear columns such as'
        ' from the one-hot encoding dummy variable trap.'
    )
    raise SingularDataError(message)
  return vifs


def _calculate_vif_using_regression(
    data: pd.DataFrame) -> List[float]:
  """Calculates VIF by regressing each feature against all other features.

  Args:
    data: Must not include the response variable (Y). Must be numeric (no
      strings or categories).

  Returns:
    vifs: list of VIFs for each feature (column) in the input data. The VIFs in
    the list are returned in the same order as the columns of the input data.
  """

  # Statsmodels expects an intercept column to give the correct results.
  data = add_constant(data, has_constant='skip')

  vifs = []
  for i, feature in enumerate(data.columns):
    if feature != 'const':
      vifs.append(variance_inflation_factor(data.values, i))

  return vifs


def calculate_vif(data: pd.DataFrame,
                  sort: bool = True,
                  use_correlation_matrix_inversion: bool = True,
                  raise_on_ill_conditioned: bool = False,
                  corr_matrix: Optional[pd.DataFrame] = None) -> pd.DataFrame:
  """Calculates Variance Inflation Factors (VIFs) of a pandas dataframe.

  VIFs are a statistical measure of multicolinearity between a set of variables.
  See https://en.wikipedia.org/wiki/Variance_inflation_factor.

  There are two approaches for computing the VIF which use different algorithms,
  but can be shown to be mathematically equivalent. One method uses matrix
  inversion (where the the VIFs are the diagonals of the inverse of the
  correlation matrix of the data), and the other uses linear regression (where
  the VIFs for each feature are defined as 1/(1-R^2) using the coefficient of
  regression for that feature against all other features).

  The matrix inversion method is generally much faster than the regression
  method. However, the correlation matrix must not be singular in order for this
  method to work; if the correlation matrix cannot be inverted we cannot compute
  the diagonals of its inverse!

  We expect there to be a few general classes of reasons why the correlation
  matrix might be singular:
     1. Two or more identical columns
     2. Two or more perfectly multicollinear columns
     3. Two or more columns that add up to a constant (often encountered when
     one-hot encoding and forgetting to drop one of the columns)
     4. A column with zero variance
     5. More columns than rows in the original data

  Columns with the above issues should be dropped before computing VIFs (and
  generally should also be dropped before using the data for statistical
  inference). Identifying these issues in the data is left to the user.

  If there are issues which are not as severe as the issues listed above, such
  as nearly identical or nearly perfectly multicollinear columns, then this
  algorithm will run, but the results may be unreliable. This is because the
  correlation matrix can become "ill-conditioned", leading to numerical errors
  in the matrix inversion which can be similar in magnitude to the VIFs
  themselves.

  This function uses numpy.linalg.solve() to compute the inverse of the
  correlation matrix, since this function is supposed to be less sensitive to
  numerical issues than the numpy.linalg.inverse() function.
  numpy.linalg.solve() uses the LAPACK _gesv routine to perform the calculation.
  According to the LAPACK users guide
  (https://www.netlib.org/lapack/lug/node80.html), the simple gesv driver has an
  approximate error bound which (in non-singular cases) works out to
  Err <= (machine precision) / (reciprocal condition number).
  Setting this ratio to unity, and noting (footnote 4.10 on that page) that
  their approximate error bounds can be underestimated by roughly a factor of
  10, we want

    (condition number) < 0.1 / (machine precision)

  in order for the VIF to be considered reliable, and so we throw an
  IllConditionedData warning/error for a condition number higher than this.

  The user also has the option to specify the regression method instead of the
  matrix inversion method, by setting to False the optional argument
  use_correlation_matrix_inversion. This approach is generally much slower.
  However, if the input dataset has significant issues that would produce a
  singular correlation matrix, the regression method will not crash or raise an
  error, though it can output a VIF of np.inf in such cases. If this happens,
  the feature(s) with infinite VIF should be dropped.

  Args:
    data: Must not include the response variable (Y). Must be numeric (no
      strings or categories).
    sort: If True, sorts the results by the VIFs in descending order.
    use_correlation_matrix_inversion: If True, uses correlation matrix inversion
      algorithm to optimize VIF calculations. If False, uses regression
      algorithm.
    raise_on_ill_conditioned: Whether to raise an exception if the correlation
      matrix is ill-conditioned (if True), or just throw a warning (if False).
      Only applies when use_correlation_matrix_inversion == True.
    corr_matrix: Computing the correlation matrix of the input data is somewhat
      expensive, so if the user has already computed it outside this function,
      pass it here so it will not have to be re-computed. Only applies when
      use_correlation_matrix_inversion == True.

  Returns:
    A VIF value for each feature.
  """

  assert all([pd.api.types.is_numeric_dtype(data[col]) for col in data.columns
             ]), ('All columns must be numeric. Try one-hot encoding the data.')

  if use_correlation_matrix_inversion:
    vifs = _calculate_vif_using_correlation_matrix_inversion(
        data,
        raise_on_ill_conditioned=raise_on_ill_conditioned,
        corr_matrix=corr_matrix)
  else:
    vifs = _calculate_vif_using_regression(data)

  vif_df = pd.DataFrame({'VIF': vifs, 'features': data.columns})

  if sort:
    vif_df = vif_df.sort_values(by='VIF', ascending=False)

  return vif_df
