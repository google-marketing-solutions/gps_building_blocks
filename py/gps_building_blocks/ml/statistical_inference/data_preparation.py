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

"""Module containing the InferenceData class."""

import functools
import operator
from typing import Iterable, Optional, Tuple
import warnings

import pandas as pd
from sklearn import preprocessing
from gps_building_blocks.ml.preprocessing import vif


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

  Columns with Low Variance
    * fix_low_variance

  Checking and addressing collinearity with VIF
    * address_collinearity_with_vif
  # TODO(): Add list of current available methods for each part.

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

    data.address_low_variance()

    data.address_collinearity_with_vif()

    data.data_check(raise_on_error=True)

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
    self._checked_low_variance = False
    self._checked_collinearity = False

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
      strategy: str = 'quick',
      min_frequency: int = 2
      ) -> pd.DataFrame:
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

    To avoid overfitting and recover the underlying signals, rare or infrequent
    fixed effect group should be removed from the study. You can choose the
    minimum frequency a fixed effect group should have using the `min_frequency`
    argument. Default value is set to `2`, meaning groups with only one
    occurrence will be removed.

    [1]
    https://econpapers.repec.org/article/ecmemetrp/v_3a46_3ay_3a1978_3ai_3a1_3ap_3a69-85.htm

    Args:
      control_columns: List of columns you want to use as control for you
        experiment.
      strategy: Options between 'quick' or 'dummy' strategy to apply fixed
        effect transformation to your data.
      min_frequency: Minimum frequency for a fixed effect group to be retain in
        the data. If `min_frequency=2`, every fixed effect group with only one
        observation will be removed from the data.

    Returns:
      Latest version of the data after fixed effect has been applied. When
      strategy is set to `quick`, the control columns will be appended to the
      `data` index.

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

    frequency_mask = pd.Series(self._fixed_effect_group_id)
    frequency_mask = frequency_mask.groupby(
        self._fixed_effect_group_id).transform('size').values
    frequency_mask = frequency_mask >= min_frequency
    self.data = self.data.loc[frequency_mask]
    self._fixed_effect_group_id = self._fixed_effect_group_id[frequency_mask]

    demean_columns = [
        column for column in self.data if column not in control_columns]
    self._demean_variable_mean = self.data[demean_columns].mean()
    self._demean_group_mean = self.data[demean_columns].groupby(
        self._fixed_effect_group_id).transform('mean')
    self.data[demean_columns] -= self._demean_group_mean
    self.data[demean_columns] += self._demean_variable_mean

    self.data = self.data.set_index(self._control_columns, append=True)
    self._has_control_factors = True
    self._control_strategy = strategy

    return self.data

  def _check_low_variance(self, raise_on_error: bool = True) -> None:
    """Verifies if data contains columns with low variance."""
    if not self._checked_low_variance:
      message = ('The data may contain columns with low variance. Consider '
                 'using `address_low_variance` identifying the columns with low'
                 'variance and whether to drop those.')

      if raise_on_error:
        raise LowVarianceError(message)
      else:
        warnings.warn(LowVarianceWarning(message))

  def address_low_variance(self,
                           threshold: float = 0,
                           drop: bool = True) -> pd.DataFrame:
    """Identifies low variances columns and option to drop it.

    Args:
      threshold: Threshold to use in VarianceThreshold where anything less than
        this threshold is dropped or used for warning.
      drop: Boolean to either drop columns with low variance or print message.
        By default all columns with low variance is dropped.

    Returns:
      Latest version of the data after low variance check has been applied.
    """
    # TODO(): Address boolean and categorical columns
    covariates = self.data.drop(columns=self.target_column)
    covariates_normalized = pd.DataFrame(
        preprocessing.scale(covariates), columns=covariates.columns)
    column_var_bool = covariates_normalized.var() > threshold
    columns_to_delete = column_var_bool[~column_var_bool].index.to_list()

    if columns_to_delete:
      if drop:
        self.data = self.data.drop(columns=columns_to_delete)
      else:
        columns_to_delete = ' , '.join(columns_to_delete)
        message = (f'Consider removing the following columns: '
                   f'{columns_to_delete}')
        warnings.warn(LowVarianceWarning(message))

    self._checked_low_variance = True

    return self.data

  def _check_collinearity(self, raise_on_error: bool = True) -> None:
    """Verifies if data has been checked for collinearity."""
    if not self._checked_collinearity:
      message = ('The data may contain collinearity between covariates. '
                 'Consider using `address_collinearity_with_vif` to identify '
                 'columns that are collinear and whether to drop them.')

      if raise_on_error:
        raise CollinearityError(message)
      else:
        warnings.warn(CollinearityWarning(message))

  def address_collinearity_with_vif(self,
                                    vif_threshold: int = 10,
                                    drop: bool = True) -> pd.DataFrame:
    """Uses VIF to identify columns that are collinear and option to drop them.

    Args:
      vif_threshold: Threshold to identify which columns have high collinearity
        and anything higher than this threshold is dropped or used for warning.
      drop: Boolean to either drop columns with high vif or print message,
        default is set to True.

    Returns:
      Data after collinearity check with vif has been applied. When drop=True
        columns with high collinearity will not be present in the returned data.
    """
    covariates = self.data.drop(columns=self.target_column)
    columns_to_drop = []

    while True:
      tmp_data = covariates.drop(columns=columns_to_drop)
      vif_data = vif.calculate_vif(tmp_data, sort=True).reset_index(drop=True)
      if vif_data.VIF[0] < vif_threshold:
        break
      columns_to_drop.append(vif_data.features[0])

    if drop:
      self.data = self.data.drop(columns=columns_to_drop)
    else:
      message = (
          f'Consider removing the following columns due to collinearity: '
          f'{columns_to_drop}')
      warnings.warn(CollinearityWarning(message))

    self._checked_collinearity = True

    return self.data

  def get_data_and_target(self) -> Tuple[pd.DataFrame, pd.Series]:
    """Returns the modelling data and the target."""
    target = self.data[self.target_column]
    data = self.data.drop(self.target_column, axis=1)
    return data, target
