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

import copy
import functools
import operator
from typing import Iterable, Iterator, List, Optional, Text, Tuple, Union
import warnings

import numpy as np
import pandas as pd
from sklearn import model_selection
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


class CategoricalCovariateError(InferenceDataError):
  pass


class CategoricalCovariateWarning(InferenceDataWarning):
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
warnings.simplefilter('always', InferenceDataWarning)


class InferenceData():
  """Data container to be used in a statistical inference analysis.

  The InferenceData container breaks down the data prepartion for statistical
  inference analysis into four parts. For any of these part, the InferenceData
  container provides internal `checks` and `methods` to address these. These
  parts are:

  * Missing Values
      Missing information should be addressed as most models will not work with
      missing values.
  * Identify categorical variables and one-hot encode
      Check if categorical columns exist in the data. If they are meant to be
      covariates in the model, these columns should be one-hot encoded into
      dummies.
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
        data=[[0.0, 1.0, 'a', 10.0],
              [0.0, 1.0, 'b', 10.0],
              [1.0, 1.0, 'c', 5.00],
              [1.0, 0.0, 'd', 0.00]],
        columns=['control', 'variable_1', 'variable_2', outcome'])

    data = inference.InferenceData(
        initial_data=some_data,
        target_column='outcome')

    data.encode_categorical_covariates(
        covariate_columns_to_encode=['variable_2']
    )

    data.control_with_fixed_effect(
        ['control'], strategy='quick', min_frequency=2)

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

  def _check_categorical_covariates(self,
                                    raise_on_error: bool = True) -> None:
    """Checks if data have any categorical covariates."""
    covariates = self.data.drop(columns=self._control_columns)
    categorical_columns = covariates.select_dtypes(
        include='object').columns.to_list()

    if categorical_columns:
      categorical_columns = ' , '.join(categorical_columns)
      message = (f'These are the categorical covariate columns in the data: '
                 f'[{categorical_columns}]. Use `encode_categorical_covariates`'
                 ' to one-hot encode these columns before moving further.')

      if raise_on_error:
        raise CategoricalCovariateError(message)
      else:
        warnings.warn(CategoricalCovariateWarning(message))

  def encode_categorical_covariates(
      self, columns: List[Text]) -> pd.DataFrame:
    """One-hot encode model covariates that are categorical.

    The control columns can be categorical because it will only be used for
    demeaning and removed before model function is applied to data. Covariates
    and Target columns must be all numeric for model function to work properly.

    Args:
      columns: List of covariate column names that will be
        transformed using one-hot encoding.

    Returns:
      Latest version of the data after one-hot encoding applied.
    """
    self.data = pd.get_dummies(
        self.data, columns=columns, dtype=int)

    return self.data

  def discretize_numeric_covariate(
      self,
      covariate_name: str,
      equal_sized_bins: bool = False,
      bins: int = 4,
      numeric: bool = False):
    """Transforms a continuous variable into a set bins.

    This useful for segmenting continuous variables to a categorical variable.
    For example when converting ages to groups of age ranges.

    Args:
      covariate_name: Name of the column to transform.
      equal_sized_bins: Whether you want to create bins with equal number of
        observations (when set to `True`) or segmenting in equal interval
        looking at the values range (when set to `False`).
      bins: Number of bins to create.
      numeric: Whether the results of the transformation should be an integer or
        a one-hot-encoding representation of the categorical variables
        generated. Returning a numeric could be convenient as it would preserve
        the "natural" ordering of the variable. For example for age ranges, with
        "16-25" encoded as `1` and "26-35" encoded as `2` would preserve the
        ordering which would be lost otherwise in a one-hot encoding.

    Returns:
      Latest version of the data the the selected covariate transformed.
    """
    cut_kwargs = {'labels': False if numeric else None, 'duplicates': 'drop'}

    if equal_sized_bins:
      buckets = pd.qcut(self.data[covariate_name], q=bins, **cut_kwargs)
    else:
      buckets = pd.cut(self.data[covariate_name], bins=bins, **cut_kwargs)

    self.data[covariate_name] = buckets

    if not numeric:
      self.data = pd.get_dummies(
          self.data, columns=[covariate_name], prefix=covariate_name)

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

  def control_with_fixed_effect(
      self,
      control_columns: Iterable[str],
      strategy: str = 'quick',
      min_frequency: int = 2
      ) -> pd.DataFrame:
    """Control external categorical variables with Fixed Effect methodology.

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
    occurrence will be removed. Make sure your control variables are categorical
    as any infrequent combination will be removed.

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
    self._check_categorical_covariates()

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
                           drop: bool = True,
                           minmax_scaling: bool = False) -> pd.DataFrame:
    """Identifies low variances columns and option to drop it.

    Features with a variance below the threshold are identified and dropped if
    requested. The data is expected to be normalised to ensure variances can be
    compared across features. The data can be normalised with MinMax scaling on
    the fly setting minmax_scaling=True.

    Args:
      threshold: Threshold to use in VarianceThreshold where anything less than
        this threshold is dropped or used for warning. If 0, drops constants.
        The maximum variance possible is .25 if MinMax scaling is applied.
      drop: Boolean to either drop columns with low variance or print message.
        By default all columns with low variance is dropped.
      minmax_scaling: If False (default) no scaling is applied to the data and
        it is expected that the user has done the appropriate normalization
        before. If True, MinMax scaling is applied to ensure variances can be
        compared across features.

    Returns:
      Latest version of the data after low variance check has been applied.
    """
    # TODO(): Address boolean and categorical columns
    covariates = self.data
    if self.target_column:
      covariates = covariates.drop(columns=self.target_column)

    if minmax_scaling:
      covariates = pd.DataFrame(
          preprocessing.minmax_scale(covariates), columns=covariates.columns)
      if not 0 <= threshold <= .25:
        message = (
            'The threshold should be between 0 and .25, with .25 being the',
            ' maximum variance possible, leading to all columns being dropped.')
        warnings.warn(LowVarianceWarning(message))
    variances = covariates.var(ddof=0)
    unique_variances = variances.unique()
    if all(
        np.isclose(variance, 0) or np.isclose(variance, 1)
        for variance in unique_variances):
      message = ('All features have a variance of 1 or 0. Please ensure you',
                 ' do not z-score your data before running this step.')
      warnings.warn(LowVarianceWarning(message))
    column_var_bool = variances > threshold
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
                                    sequential: bool = True,
                                    interactive: bool = False,
                                    drop: bool = True) -> pd.DataFrame:
    """Uses VIF to identify columns that are collinear and option to drop them.

    You can customize how collinearity will be resolved with `sequential` and
    `interactive` parameters. By default, the VIF score will re-calculated every
    time the column with the highest VIF score is dropped until the threshold is
    met. If you wish to remove all the columns with VIF score higher than the
    threshold, you can set `sequential=False`.
    If you want to have a say on which column is going to removed, rather than
    automatically pick the column with the highest VIF score, you can set
    `interactive=True`. This will prompt for your input every time columns are
    found with VIF score higher than your threshold, whether `sequential` is set
    to True of False.

    Args:
      vif_threshold: Threshold to identify which columns have high collinearity
        and anything higher than this threshold is dropped or used for warning.
      sequential: Whether you want to sequentially re-calculate VIF each time
        after a set column(s) have been removed or only once.
      interactive: Whether you want to manually specify which column(s) you want
       to remove.
      drop: Boolean to either drop columns with high VIF or print message,
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

      if vif_data['VIF'][0] < vif_threshold:
        break

      if interactive:
        selected_columns = _vif_interactive_input_and_validation(vif_data)
      elif sequential:
        selected_columns = [vif_data['features'][0]]
      else:
        vif_filter = vif_data['VIF'] >= vif_threshold
        selected_columns = vif_data['features'][vif_filter].tolist()

      columns_to_drop.extend(selected_columns)

      if not sequential or not selected_columns:
        break

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

  def _copy_and_index_inference_data(self,
                                     indices: np.ndarray) -> 'InferenceData':
    """Deep-Copies an InferenceData object on indices provided.

    It does a deepcopy of the current object and indexes both self.data and
    self.initial_data.

    Args:
      indices: List of indices to keep in the data.

    Returns:
      InferenceData with the data indicated by indices.
    """
    subset_copy = copy.deepcopy(self)
    subset_copy.initial_data = self.initial_data.take(indices)
    subset_copy.data = self.data.take(indices)
    return subset_copy

  def split(
      self,
      cross_validation: Union[int,
                              model_selection.BaseCrossValidator,
                              model_selection.ShuffleSplit,
                              model_selection.StratifiedShuffleSplit],
      groups: Optional[np.ndarray] = None,
      ) -> Iterator[Tuple['InferenceData', 'InferenceData']]:
    """Splits the data using the indicated cross validator.

    Args:
      cross_validation: Cross validation to be applied. If an int is passed
        and groups is None a sklearn Kfold is used with cross_validation as
        the number of splits. If an int is passed and groups is not None,
        sklearn GroupKFold will be used. Whena a cross validator is passed it
        is used directly.
      groups: If cross validating for non overlaping groups, this array
        indicates to which group each row belongs.

    Yields:
      A tuple with train and test InferenceDatas.
    """
    if isinstance(cross_validation, int):
      if groups is not None:
        cross_validation = model_selection.GroupKFold(n_splits=cross_validation)
      else:
        cross_validation = model_selection.KFold(n_splits=cross_validation)

    for train_index, test_index in cross_validation.split(self.data,
                                                          groups=groups):
      train_inference_data = self._copy_and_index_inference_data(train_index)
      test_inference_data = self._copy_and_index_inference_data(test_index)
      yield train_inference_data, test_inference_data


def _input_mock(promp_message: str) -> str:
  """Allows 'input' to be mocked with nose test."""
  # https://stackoverflow.com/questions/25878616/attributeerror-none-does-not-have-the-attribute-print
  return input(promp_message)


def _print_mock(message: str) -> None:
  """Allows 'print' to be mocked with nose test."""
  # https://stackoverflow.com/questions/25878616/attributeerror-none-does-not-have-the-attribute-print
  print(message)


def _vif_interactive_input_and_validation(vif_data: pd.DataFrame) -> List[str]:
  """Prompts and validates column selection for interactive sessions."""
  while True:
    _print_mock(vif_data.set_index('features'))
    selected_columns = _input_mock(
        'Select one or more variables to remove separated by comma. '
        'To end the interactive session press Enter.\n')

    if not selected_columns:
      return []

    selected_columns = selected_columns.split(',')

    valid_selection = True
    valid_columns = vif_data['features'].tolist()
    for column in selected_columns:
      if column not in valid_columns:
        _print_mock(f'Invalid column "{column}". '
                    f'Valid columns: {",".join(valid_columns)}')
        valid_selection = False

    if valid_selection:
      return selected_columns
