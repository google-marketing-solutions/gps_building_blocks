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

"""Module containing the InferenceData and InferenceModel classes."""

import functools
import operator
from typing import Iterable, Optional
import warnings

import pandas as pd


class InferenceDataError(Exception):
  pass


class InferenceDataWarning(Warning):
  pass


class MissingValueError(InferenceDataError):
  pass


class MissingValueWarning(InferenceDataWarning):
  pass


class ControlVariableError(InferenceDataError):
  pass


class ControlVariableWarning(InferenceDataWarning):
  pass


class CollinearityError(InferenceDataError):
  pass


class CollinearityWarning(InferenceDataWarning):
  pass


class LowVarianceError(InferenceDataError):
  pass


class LowVarianceWarning(InferenceDataWarning):
  pass

# Force custom Warnings to emitted all the time, not only once.
warnings.simplefilter('always', MissingValueWarning)
warnings.simplefilter('always', ControlVariableWarning)
warnings.simplefilter('always', CollinearityWarning)
warnings.simplefilter('always', LowVarianceWarning)


class InferenceData():
  """Data container to be used in a statistical inference analysis.

  The InferenceData container breaks down the data prepartion for statistical
  inference analysis into four parts. For any of these part, the InferenceData
  container provides internal `checks` and `methods` to address these. These
  parts are:

  * Missing Values
      Missing information should be addressed as most models will not work with
      missing values.
  * Controlling for External Factors
      These are elements that are adding noise to the signal, for example when
      comparing two different ads performance, you want to control for the
      different targeting. It’s not always possible to control for external
      factors, as some of them may not be measurable or representable in the
      data.
  * Identify columns that carry little or no information (low-variance)
      Features that carry little to no information. These should be flagged and
      potentially remove before modelling phase.
  * Addressing Collinearity
      Highly correlated features may confund the result a statistical inference
      analysis. These should be identified and potentially removed before the
      modelling phase.

  During the data preparation pipeline, if the above will not be addressed,
  `InferenceDataError` will be risen or `InferenceDataWarning` if you choose to
  ignore these messages.

  These are the current available `methods` in the DataInference container to
  address the above parts:

  Missing Values
    * impute_missing_value

  Controlling for External Factors
    * fixed_effect

  # TODO(b/174228077): Add list of current available methods for each part.

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

    data.fixed_effect(['control'], strategy='quick')

    data.check_data(raise_on_error=True)

  Attributes:
    initial_data: The initial DataFrame with control variables and features
      provided when the object is initialized.
    target_column: The name of the column in the `initial_data` to be used as
      target in your analysis. This can be binary or boolean.
    data: Latest version of the data after any transformation is applied. If no
      transformation was applied, it will be exactly the same as `initial_data`.
  """

  def __init__(
      self,
      initial_data: pd.DataFrame,
      target_column: Optional[str] = None) -> None:
    """Initializes the Inference Data.

    Args:
      initial_data: The initial DataFrame with control variables and features
        provided when the object is initialized.
      target_column: The name of the column in the `initial_data` to be used as
        target in your analysis. This can be binary or boolean.

    Raises:
      KeyError: if the target_column is missing from the initial_data provided.
    """

    self.initial_data = initial_data
    self.data = initial_data.copy()
    self.target_column = target_column
    self._has_control_factors = False

    if target_column and target_column not in initial_data:
      raise KeyError('Target "{target_column}" not in data.')

    self._check_missing_values(raise_on_error=False)

  def data_check(self, raise_on_error: bool = True) -> None:
    """Verify data integrity.

    Will perform the data checks in the following order:

    1) Check for missing values.
    2) Check that external factors are included and accounted for.
    3) Check for low-variance and constants.
    4) Check that Collinearity has been verified addressed.

    Args:
      raise_on_error: Weather to raise an exception if a problem if found with
        one of the above checks in the latest transformation of the data. If set
        to False, the integrity checks may emit InferenceDataWarning warnings.

    Raises:
      MissingValueError: If the latest transformation of the data has columns
        with missing values.
      ControlVariableError: If the latest transformation of the data hasn't gone
        thorugh a method to control for external factors.
    """
    self._check_missing_values(raise_on_error)
    self._check_control(raise_on_error)
    self._check_low_variance(raise_on_error)
    self._check_collinearity(raise_on_error)

  def _check_missing_values(self, raise_on_error: bool = True) -> None:
    """Verifies if data have no missing values."""
    missing_percentage = self.data.isnull().mean() * 100
    missing_percentage = missing_percentage[missing_percentage != 0]
    if not missing_percentage.empty:
      missing = '; '.join(f'{name}: {percentage:.2f}%' for name, percentage
                          in missing_percentage.to_dict().items())
      message = f'The data has the following missing values ({missing})'

      if raise_on_error:
        raise MissingValueError(message)

      warnings.warn(MissingValueWarning(message))

  def impute_missing_values(self, strategy: str = 'mean') -> pd.DataFrame:
    """Imputes any missing value with their mean or median.

    Replaces the missing values with their `mean` or `median`. If more complex
    operations are needed to impute missing values, these needs be executed in
    the initial data before creating the InferenceData object.

    Args:
      strategy: If strategy is 'mean', will replace the missing values with
        their means. For any other values, the 'median' will be used.

    Returns:
      Latest version of the data after missing value imputation is applied.
    """
    if strategy == 'mean':
      impute_values = self.data.mean()
    else:
      impute_values = self.data.median()

    self.data = self.data.fillna(impute_values)

    return self.data

  def _check_control(self, raise_on_error: bool = True)  -> None:
    """Verifies if data is controlling for external variables."""
    if not self._has_control_factors:
      message = ('The data is not controlling for external factors. Consider '
                 'using `fixed_effect` indicating the columns to use as control'
                 'for external factors.')

      if raise_on_error:
        raise ControlVariableError(message)
      else:
        warnings.warn(ControlVariableWarning(message))

  def fixed_effect(
      self,
      control_columns: Iterable[str],
      strategy: str = 'quick') -> pd.DataFrame:
    """Fixed effect with the strategy using the control columns.

    Fixed effects mitigate the confounding factors and help restore the
    underlying signal. Fixed Effects is widely used to estimate causal effects
    using observational data. It is designed to control for differences across
    individuals and/or time which could confound estimation of the variable of
    interest on an outcome variable.

    Originally, Fixed Effect model are implemented using Least Squares Dummy
    Variable model (LSDV), which essentially uses a dummy variable for each
    fixed effect. This option is available setting `strategy = 'dummy'`. When
    the number of fixed effects is large it is easy to incur in memory issues
    and some model may struggle to handle a very highly dimensional space. We
    can transform the data de-meaning each fixed effect, subtracting the fixed
    effect group mean and adding back the overall mean. Mundlak (1978)[1] has
    shown that this efficient fixed effects implementation is equivalent to a
    LSDV approach. You can use this efficient transformation setting the
    parameter `strategy = 'quick'`.

    [1]
    https://econpapers.repec.org/article/ecmemetrp/v_3a46_3ay_3a1978_3ai_3a1_3ap_3a69-85.htm

    Args:
      control_columns: List of columns you want to use as control for you
        experiment.
      strategy: Options between 'quick' or 'dummy' strategy to apply fixed
        effect transformation to your data.

    Returns:
      Latest version of the data after fixed effect has been applied.

    Raises:
      NotImplementedError: Currently, only the 'quick' strategy is available.
      Setting `strategy` to any other value will raise this exception.
    """
    if strategy != 'quick':
      raise NotImplementedError(
          "Only 'quick' fixed effect is currently implemented.")

    self._control_columns = control_columns
    self._fixed_effect_group_id = functools.reduce(
        operator.add, self.data[control_columns].astype(str).values.T)

    demean_columns = [
        column for column in self.data if column not in control_columns]
    self._demean_group_mean = self.data[demean_columns].groupby(
        self._fixed_effect_group_id).transform('mean')
    self.data[demean_columns] -= self._demean_group_mean
    self.data[demean_columns] += self.data[demean_columns].mean()

    self._has_control_factors = True

    return self.data

  def _check_low_variance(self, raise_on_error: bool = True) -> None:
    """Verifies if low variances variables has been addressed in the data."""
    # TODO(b/173768759): Check low-variance and constants.
    pass

  def _check_collinearity(self, raise_on_error: bool = True) -> None:
    """Verifies if collinearity has been addressed in the data."""
    # TODO(b/173768760): Check for collinearity.
    pass
