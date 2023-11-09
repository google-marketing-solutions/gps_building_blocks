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
import enum
import functools
import operator
from typing import Iterable, Iterator, List, Optional, Tuple, Union
import warnings

import numpy as np
import pandas as pd
from sklearn import model_selection
from sklearn import preprocessing

from gps_building_blocks.ml.preprocessing import vif


class MergeMethod(enum.Enum):
  """Options for merge_method in address_collinearity_with_vif()."""

  AND = 'and'
  OR = 'or'

  def merge_separator(self) -> str:
    """Returns the merge separator."""
    return f'_{self.value.upper()}_'


class VifMethod(enum.Enum):
  """Options for the vif_method argument in address_collinearity_with_vif()."""

  SEQUENTIAL = 'sequential'
  INTERACTIVE = 'interactive'
  QUICK = 'quick'
  SEQUENTIAL_MERGE = 'sequential_merge'


class VifColumnRemover:
  """Merges or drops columns in a dataframe.

  Used in address_collinearity_with_vif() to keep track of the columns to be
  merged or dropped.

  Attributes:
    columns_to_merge: A list of sets of column names that are to be merged. All
      columns are assumed to be numeric.
    columns_to_drop: A list of sets of column names that are to be dropped.
    vif_method: The vif method to be applied, which indicates whether the
      columns should be merged or dropped.
    merge_method: The method used to aggregate the values in the merge columns.
    all_selected_columns: All the columns to be merged or dropped.
  """

  merge_method: MergeMethod
  vif_method: VifMethod
  columns_to_merge: dict[str, set[str]]
  columns_to_drop: list[str]

  def __init__(
      self,
      vif_method: VifMethod,
      merge_method: MergeMethod | str = MergeMethod.OR,
  ):
    """Initializes the merge columns.

    Args:
      vif_method: The vif method being used, which determines if the columns are
        dropped or merged.
      merge_method: The method used to aggregate the values in the merge columns
        (defaults to 'or').
    """
    self.merge_method = MergeMethod(merge_method)
    self.vif_method = VifMethod(vif_method)
    self.columns_to_merge = {}
    self.columns_to_drop = []

  def is_merging(self) -> bool:
    return self.vif_method == VifMethod.SEQUENTIAL_MERGE

  def is_dropping(self) -> bool:
    return not self.is_merging()

  def _merge_function(self, input_values: np.ndarray) -> int:
    if self.merge_method == MergeMethod.AND:
      return np.min(input_values)
    elif self.merge_method == MergeMethod.OR:
      return np.max(input_values)
    else:
      raise RuntimeError(f"Merge method '{self.merge_method}' is unexpected.")

  @property
  def all_selected_columns(self):
    return functools.reduce(
        lambda x, y: x | y, self.columns_to_merge.values(), set()
    )

  def _add_columns_to_merge(self, columns: list[str]) -> None:
    """Adds columns to be merged."""

    columns = set(columns)
    columns_already_merged = self.all_selected_columns & columns
    if columns_already_merged:
      raise RuntimeError(
          'Trying to merge the following columns that have already been '
          f'merged: {columns_already_merged}'
      )

    new_group = set()
    for new_column_to_merge in columns:
      new_column_to_merge = self.columns_to_merge.pop(
          new_column_to_merge, {new_column_to_merge}
      )
      new_group.update(new_column_to_merge)

    self.columns_to_merge[self.get_merged_column_name(new_group)] = new_group

  def add_columns(self, columns: list[str]) -> None:
    """Adds a set of columns to be merged or dropped.

    If dropping:
      The columns will be appended to the list of columns to drop
    If merging:
    - If all of the columns don't yet exist in columns_to_merge, then a new
    group will be added.
    - If any the columns to be merged already exist as merged columns, then a
    a new group will be created combining all of the listed merged columns.
    - If any of the columns to be merged is already in a group, an exception
    will be raised, because we cannot take a single column out of a group
    and add it to another one - doing so could lead to an endless loop of
    swapping columns between groups.

    Args:
      columns: The columns to be added to merge.

    Raises:
      RuntimeError: If the columns to be merged already exist inside another
      group
    """
    if self.is_merging():
      self._add_columns_to_merge(columns)
    else:
      self.columns_to_drop.extend(columns)

  def get_merged_column_name(self, columns: set[str]) -> str:
    """Gets the name of the merged columns.

    Args:
      columns: The column names to be merged.

    Returns:
      The name of the column they will be merged into.
    """
    merge_name_separator = self.merge_method.merge_separator()
    return merge_name_separator.join(sorted(columns))

  def _apply_merge(self, data: pd.DataFrame) -> pd.DataFrame:
    """Applies the merging to a dataframe."""

    for merge_name, merge_set in self.columns_to_merge.items():
      data[merge_name] = data[list(merge_set)].apply(
          self._merge_function, axis=1
      )
      data = data.drop(columns=list(merge_set))
    return data

  def apply(self, data: pd.DataFrame) -> pd.DataFrame:
    """Applies the merging or dropping to a dataframe.

    If merging, this will take all the columns to be merged, merge them into a
    single column using the merge method, and then drop the original columns.

    If dropping this will just drop the original columns.

    Args:
      data: The data to apply the merge to.

    Returns:
      A copy of the data with the columns merged.
    """
    data = data.copy()
    if self.is_merging():
      data = self._apply_merge(data)
    else:
      data = data.drop(columns=self.columns_to_drop)
    return data

  def __str__(self) -> str:
    if self.is_merging():
      return 'Merging: ' + str(list(self.columns_to_merge.values()))
    else:
      return 'Dropping: ' + str(self.columns_to_drop)


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


class SingularDataError(InferenceDataError):
  """Error associated with the address_collinearity_with_vif() method.

  This error is raised when the dataset has a singular or nearly singular
  correlation matrix. The solution is to reduce the condition number of the
  correlation matrix, usually by dropping one or more problematic columns from
  the input dataset. See the docstring for
  InferenceData.address_collinearity_with_vif() for more details.
  """


class SequentialMergeError(InferenceDataError):
  """Error when 'sequential_merge' is used in address_collinearity_with_vif().

  This error is raised when the we're attemping to merge two features that are
  negatively correlated. The results would be 'feature1_AND_NOT_feature2' or
  'feature1_OR_NOT_feature2' which would make the interpretation harder. Only
  positively correlated features will be merge.
  """


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
      raise KeyError(f'Target "{target_column}" not in data.')

    self._check_missing_values(raise_on_error=False)

  def data_check(self, raise_on_error: bool = True) -> None:
    """Verify data integrity.

    Will perform the data checks in the following order:

    1) Check for missing values.
    2) Check that external factors are included and accounted for.
    3) Check for low-variance and constants.
    4) Check that Collinearity has been verified addressed.

    Args:
      raise_on_error: Whether to raise an exception if a problem if found with
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
      self, columns: List[str], drop_first: bool = False) -> pd.DataFrame:
    """One-hot encode model covariates that are categorical.

    The control columns can be categorical because it will only be used for
    demeaning and removed before model function is applied to data. Covariates
    and Target columns must be all numeric for model function to work properly.

    Args:
      columns: List of covariate column names that will be
        transformed using one-hot encoding.
      drop_first: Whether to get k-1 dummies out of k categorical levels by
        removing the first level.

    Returns:
      Latest version of the data after one-hot encoding applied.
    """
    self.data = pd.get_dummies(
        self.data, columns=columns, drop_first=drop_first, dtype=int)

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

  def _demean_fixed_effects(
      self, data: pd.DataFrame, fixed_effect_group_id: pd.Series
  ) -> pd.DataFrame:
    """Applys demeaning to the data.

    Args:
      data: The data to de-mean.
      fixed_effect_group_id: The fixed effect groups to de-mean across.

    Returns:
      The de-meaned data
    """
    demean_columns = [
        column for column in data if column not in self._control_columns
    ]
    demean_variable_mean = data[demean_columns].mean()
    demean_group_mean = (
        data[demean_columns].groupby(fixed_effect_group_id).transform('mean')
    )

    data[demean_columns] -= demean_group_mean
    data[demean_columns] += demean_variable_mean
    data = data.set_index(self._control_columns, append=True)
    return data

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

    # When running address_collinearity_with_vif() using the sequential_merge
    # method, we must merge the columns before controlling for fixed effects
    # and then control the data after creating the merged column.
    # Therefore we need to make a copy here of the data before controlling
    # fixed effects.
    self._data_pre_fixed_effects = self.data.copy()
    self.data = self._demean_fixed_effects(
        self.data, self._fixed_effect_group_id
    )
    self._has_control_factors = True
    self._control_strategy = strategy

    return self.data

  def _check_low_variance(self, raise_on_error: bool = True) -> None:
    """Verifies if data contains columns with low variance."""
    if not self._checked_low_variance:
      message = ('The data has not been checked for low variance. Consider '
                 'using `address_low_variance` to identify if any column has '
                 'low variation and whether to drop these.')

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
        compared across features. Also, this will change the underlying
        covariates values if True.

    Returns:
      Latest version of the data after low variance check has been applied.
    """

    if minmax_scaling:
      to_transform = self.data.columns.drop(self.target_column,
                                            errors='ignore')
      self.data[to_transform] = preprocessing.minmax_scale(
          self.data[to_transform])
      if not 0 <= threshold <= .25:
        message = (
            'The threshold should be between 0 and .25, with .25 being the',
            ' maximum variance possible, leading to all columns being dropped.')
        warnings.warn(LowVarianceWarning(message))

    # TODO(): Address boolean and categorical columns
    covariates = self.data
    if self.target_column:
      covariates = covariates.drop(columns=self.target_column)

    variances = covariates.var(ddof=0)
    # narrow type from Series | float as covariates is definitely a DataFrame.
    assert isinstance(variances, pd.Series)  # pandas-missing-overload
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
        _print_mock(
            f'Removing low-variance columns: {",".join(columns_to_delete)}')
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
      message = ('The data has not been checked for multi-collinearity. '
                 'Consider using `address_collinearity_with_vif` to identify '
                 'columns that are collinear and whether to drop them.')

      if raise_on_error:
        raise CollinearityError(message)
      else:
        warnings.warn(CollinearityWarning(message))

  def _get_list_of_correlated_features(
      self,
      vif_data: pd.DataFrame,
      corr_matrix: pd.DataFrame,
      min_absolute_corr: float) -> List[List[str]]:
    """Generates list of features which are correlated to features in vif_data.

    Args:
      vif_data: Table of features and VIFs
      corr_matrix: (Pearson) correlation matrix for all features in the original
        dataset (can be more features than just the features in vif_data)
      min_absolute_corr: Minimum value of absolute correlation; only display
        features with absolute correlations above this value
    Returns:
      List of lists of features, along with their correlations.
    """
    correlated_features = []
    for feature in vif_data.features:
      min_correlation_mask = abs(corr_matrix[feature]) > min_absolute_corr
      correlated_features_info = corr_matrix.loc[min_correlation_mask]
      correlated_features_info = correlated_features_info.drop(feature, axis=0)
      correlated_features_info = correlated_features_info.sort_values(
          feature, ascending=False)

      correlated_features.append([
          f'{feature_name}: {round(corr_coeff, 2)}' for feature_name, corr_coeff
          in zip(correlated_features_info[feature].index,
                 correlated_features_info[feature].values)
      ])

    return correlated_features

  def _generate_vif_error_message(self,
                                  trimmed_corr_matrix: pd.DataFrame) -> str:
    """Generates error message for VIF when the correlation matrix is singular.

    Depending on whether the correlation matrix is perfectly singular or just
    nearly singular, the exception raised by vif.calculate_vif() will either be
    a SingularDataError or an IllConditionedDataError.

    This function is intended to run in a try/except clause, and should work for
    either of those two types of exception. It generates an informative error
    message for the user, to help them troubleshoot this problem.

    Args:
      trimmed_corr_matrix: Correlation matrix for the inference data, but
        trimmed to remove columns and rows corresponding to features which have
        been dropped during the address_collinearity_with_vif() process.

    Returns:
      message: error message to display.
    """
    upper_triangle_corrs = np.triu(trimmed_corr_matrix, k=1)
    sorted_indices = np.unravel_index(
        np.argsort(np.abs(upper_triangle_corrs), axis=None),
        upper_triangle_corrs.shape)
    message_parts = [(
        'Inference Data has a singular or nearly singular correlation matrix. '
        'This could be caused by extremely correlated or collinear columns. '
        'The three pairs of columns with the highest absolute correlation '
        'coefficients are: ')]
    top3_pairs = []
    for i in range(3):
      col1 = trimmed_corr_matrix.columns[sorted_indices[0][-1 - i]]
      col2 = trimmed_corr_matrix.columns[sorted_indices[1][-1 - i]]
      corr = trimmed_corr_matrix.iloc[sorted_indices[0][-1 - i],
                                      sorted_indices[1][-1 - i]]
      top3_pairs.append(f'({col1},{col2}): {corr:0.3f}')

    message_parts.append(', '.join(top3_pairs) + '. ')

    if not self._checked_low_variance:
      message_parts.append(
          'This could also be caused by columns with extremiely low variance. '
          'Recommend running the address_low_variance() method before VIF. ')
    message_parts.append(
        'Alternatively, consider running address_collinearity_with_vif() with '
        'use_correlation_matrix_inversion=False to avoid this error.')
    return ''.join(message_parts)

  def _validate_data_for_vif_method(self, vif_method: VifMethod) -> None:
    """Checks that the data is valid for the selected vif_method.

    This checks that for sequential_merge all the features are binary.

    Args:
      vif_method: The vif method to check for.
    """
    if vif_method == VifMethod.SEQUENTIAL_MERGE:
      if self._has_control_factors:
        data_to_validate = self._data_pre_fixed_effects.drop(
            columns=self._control_columns
        )
      else:
        data_to_validate = self.data

      if self.target_column:
        data_to_validate = data_to_validate.drop(columns=self.target_column)

      all_columns_are_binary = np.all(data_to_validate.isin([0, 1]).values)
      if not all_columns_are_binary:
        raise ValueError(
            "The 'sequential_merge' vif method is only applicable if all "
            'features are binary.'
        )

  def address_collinearity_with_vif(
      self,
      vif_method: Union[str, VifMethod] = 'sequential',
      vif_threshold: int = 10,
      drop: bool = True,
      use_correlation_matrix_inversion: bool = True,
      raise_on_ill_conditioned: bool = True,
      min_absolute_corr: float = 0.4,
      handle_singular_data_errors_automatically: bool = True,
      merge_method: str | MergeMethod = 'or',
  ) -> pd.DataFrame:
    """Uses VIF to identify columns that are collinear and optionally drop them.

    The 'vif_method' argument specifies the control flow for the variance
    inflation factor analysis. It can be either 'sequential', 'interactive', or
    'quick'.

    * sequential: Sequentially remove a column with the highest VIF value until
    all columns meet the vif_threshold.
    * interactive: Same as `sequential` but the user is prompted which column(s)
    to remove at each iteration.
    * quick: Remove all columns with VIF value greater than vif_threshold.
    * sequential_merge: Sequentially merge a column with the highest VIF value
    with the columns it's most correlated with until all columns meet the
    vif_threshold.

    To remove problematic collinear features, the 'sequential' method performs
    the VIF analysis iteratively, removing the column with the highest VIF each
    time until all the columns meet the `vif_threshold` without user input.

    If you want to manually decide which column or columns you want to drop in
    each iteration, you can choose the 'interactive' method to be prompted to
    screen the columns to remove. To assist in choosing which column(s) to drop,
    a list of correlated features will be shown having at least a minimum
    correlation of `min_absolute_corr`.

    As alternative to the iterative methods, a quicker solution is to remove all
    columns having VIF value higher than `vif_threshold`. In this case the VIF
    analysis will be performed only once. Note that removing multiple variables
    at once like this leads to removing variables that otherwise wouldn't be
    removed using the sequential approach.

    The 'sequential_merge' method works similarly to 'sequential', but instead
    of removing columns it merges the column with the highest VIF with its most
    correlated column until all columns meet the vif_threshold. This method is
    only applicable for datasets where all features are binary (1/0 or
    True/False), or categorical that have been one hot encoded. By default the
    columns are merged by the "or" method, which will set the merged column to
    1 if any of the columns being merged are 1. Alternatively you can set the
    merge method to "and", which will set the merged column to 1 only if all of
    the columns to be merged are 1. In practice, as these columns should be
    highly correlated anyway, the choice should make little difference. If you
    have controlled your data with fixed effects, the VIF and correlations are
    calculated on the de-meaned data, and each time a set of columns are merged
    the de-meaning is re-applied.


    Args:
      vif_method: Specify the control flow for the analysis. It can be either
        'sequential', 'quick', 'interactive' or 'sequential_merge'.
      vif_threshold: Threshold to identify which columns have high collinearity
        and anything higher than this threshold is dropped or used for warning.
      drop: Boolean to either drop or merge columns with high VIF or just print
        a message, default is set to True.
      use_correlation_matrix_inversion: If True, uses correlation matrix
        inversion algorithm to optimize VIF calculations. If False, uses
        regression algorithm.
      raise_on_ill_conditioned: Whether to raise an exception if the correlation
        matrix is ill-conditioned. Only applies when
        use_correlation_matrix_inversion=True.
      min_absolute_corr: Minimum absolute correlation required to display a
        feature as "correlated" to another feature in interactive mode. Only
        applies when vif_method='interactive'. Should be between 0 and 1, though
        this is not currently enforced.
      handle_singular_data_errors_automatically: If True, then
        SingularDataErrors and IllConditionedDataErrors from vif.calculate_vif()
        will be handled automatically by injecting artifical noise into the data
        and re-running. Note that the data with artifical noise is an
        intermediate product of this method (tmp_data) and the output of the
        method does not contain that artifical noise. The noise is random noise
        following a normal distribution, with standard deviation for each column
        defined by the standard deviation of the data in that column multiplied
        by the fraction fractional_noise_to_add_per_iteration (set to 1e-4). To
        avoid getting stuck in an infinite loop, only max_number_of_iterations
        (set to 1000) of the noise injection procedure are allowed; after this
        number of iterations if the correlation matrix is still singular, the
        method fails with a SingularDataError. This argument is only relevant
        when use_correlation_matrix_inversion=True.
      merge_method: If vif_method='sequential_merge', this is the method used to
        merge the columns together. Must be one of 'or' or 'and'.

    Returns:
      Data after collinearity check with vif has been applied. When drop=True,
        columns with high collinearity will not be present in the returned data.

    Raises:
      SingularDataError: Raised when use_correlation_matrix_inversion=True and
        the correlation matrix of self.data is singular or ill-conditioned.
        Also raised when use_correlation_matrix_inversion=True and
        handle_singular_data_errors_automatically=True, if the random noise
        injected into the data was not sufficient to resolve the problem.
      ValueError: Raised when vif_method is not one of the four expected values
        ('sequential', 'quick', 'interactive' or 'sequential_merge').
      LowVarianceError: If any column in the data have constant value. Use
        `InferenceData.address_low_variance` to remove constant columns.
      SequentialMergeError: If at any point of the VIF iteration it will try to
        merge two columns that are inversely correlated. This when you have
        redundant features like "is_male" and "is_not_male" or other instances
        where the columns are highly correlated.
      RuntimeError: Raised if the merging process attempts to merge the same
        column twice.
    """
    vif_method = VifMethod(vif_method)
    self._validate_data_for_vif_method(vif_method)
    columns_for_vif = VifColumnRemover(vif_method, merge_method)

    if self._has_control_factors:
      covariates = self._data_pre_fixed_effects[
          self.data.columns.values.tolist() + self._control_columns
      ]
    else:
      covariates = self.data

    if self.target_column:
      covariates = covariates.drop(columns=self.target_column)

    fractional_noise_to_add_per_iteration = 1.0e-4
    max_number_of_iterations = 1000

    while True:
      tmp_data = columns_for_vif.apply(covariates)
      if self._has_control_factors:
        tmp_data = self._demean_fixed_effects(
            tmp_data, self._fixed_effect_group_id
        )
      corr_matrix_for_vif = tmp_data.corr()

      if handle_singular_data_errors_automatically:
        rng = np.random.default_rng()
        variances_for_each_column = tmp_data.var(ddof=0, axis=0)
        if not isinstance(variances_for_each_column, pd.Series):
          raise RuntimeError(
              'variances_for_each_column should always be a pandas series'
          )

        variance_df = pd.DataFrame(
            data=[variances_for_each_column.to_list()] * tmp_data.shape[0]
        )

      vif_succeeded_flag = False
      for iteration_count in range(max_number_of_iterations):
        if iteration_count > 0:
          corr_matrix_for_vif = tmp_data.corr()

        constant_columns = corr_matrix_for_vif.isnull().all()
        # pandas-missing-overload
        assert isinstance(constant_columns, pd.Series)
        if constant_columns.any():
          # If there are NaN in the correlation matrix, this means that that are
          # columns that are constants. If this happens, we should remind the
          # user to run address_low_variance method beforehand.
          offending_columns = ', '.join(
              constant_columns[constant_columns].index)
          raise LowVarianceError(
              f'The column(s) ({offending_columns}) have constant value(s) and '
              'should not be considered for modelling. Please consider using '
              '`InferenceData.address_low_variance` to remove them.')

        try:
          vif_data = vif.calculate_vif(
              tmp_data,
              sort=True,
              use_correlation_matrix_inversion=use_correlation_matrix_inversion,
              raise_on_ill_conditioned=raise_on_ill_conditioned,
              corr_matrix=corr_matrix_for_vif)
          vif_succeeded_flag = True
        except (vif.SingularDataError, vif.IllConditionedDataError):
          message_postscript = ''
          if handle_singular_data_errors_automatically:
            if iteration_count < max_number_of_iterations - 1:
              noise = rng.normal(
                  size=tmp_data.shape,
                  scale=np.sqrt(variance_df) *
                  fractional_noise_to_add_per_iteration)
              tmp_data += noise
              continue
            else:
              message_postscript = (
                  ' Automatic attempt to resolve SingularDataError by '
                  'injecting artifical noise to the data has failed. This '
                  'probably means the dataset has too many features relative '
                  'to the number of samples.')

          message = self._generate_vif_error_message(corr_matrix_for_vif)
          message += message_postscript
          raise SingularDataError(message)

        if vif_succeeded_flag:
          break
      if max(vif_data['VIF']) < vif_threshold:
        break

      if vif_method == VifMethod.INTERACTIVE:
        correlated_features = self._get_list_of_correlated_features(
            vif_data,
            corr_matrix_for_vif.drop(
                columns_for_vif.all_selected_columns, axis=0
            ).drop(columns_for_vif.all_selected_columns, axis=1),
            min_absolute_corr,
        )
        vif_data['correlated_features'] = correlated_features
        selected_columns = _vif_interactive_input_and_validation(vif_data)
      elif vif_method == VifMethod.SEQUENTIAL_MERGE:
        max_vif_column = vif_data.iloc[0].features
        # Take the second highest correlation, because the highest is the
        # correlation with itself (below).
        max_corr = (
            corr_matrix_for_vif[max_vif_column]
            .sort_values(ascending=False)
            .values[1]
        )

        max_corr_mask = corr_matrix_for_vif[max_vif_column] == max_corr
        selected_columns = corr_matrix_for_vif.loc[
            max_corr_mask
        ].index.values.tolist()
        selected_columns.append(max_vif_column)

        if max_corr < 0:
          raise SequentialMergeError(
              f'Cannot merge ({", ".join(selected_columns)}) as they are '
              f'inversely correlated: {max_corr:3f}. Currently this is not '
              'supported. Please try using vif_method="sequential".')
      elif vif_method == VifMethod.SEQUENTIAL:
        selected_columns = [vif_data.iloc[0].features]
      else:
        vif_filter = vif_data['VIF'] >= vif_threshold
        selected_columns = vif_data['features'][vif_filter].tolist()

      columns_for_vif.add_columns(selected_columns)

      if (vif_method == VifMethod.QUICK) or not selected_columns:
        break

    if drop:
      print(columns_for_vif)
      final_data = columns_for_vif.apply(covariates)
      if self._has_control_factors:
        final_data = self._demean_fixed_effects(
            final_data, self._fixed_effect_group_id
        )
      if self.target_column:
        final_data[self.target_column] = self.data[self.target_column]
      self.data = final_data
    else:
      if vif_method == VifMethod.SEQUENTIAL_MERGE:
        message = (
            'Consider merging the following column groups due to collinearity:'
            f' {columns_for_vif}'
        )
      else:
        message = (
            'Consider removing the following columns due to collinearity: '
            f'{columns_for_vif}'
        )
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
  with pd.option_context('display.max_colwidth', None):
    print(message)


def _vif_interactive_input_and_validation(vif_data: pd.DataFrame,
                                          max_features_to_display: int = 10
                                         ) -> List[str]:
  """Prompts and validates column selection for interactive sessions.

  Args:
    vif_data: DataFrame of VIF data to display.
    max_features_to_display: Maximum number of features to display, in
      descending order of VIF score.

  Returns:
    selected_columns: A list (can be empty) of column names to remove.
  """

  while True:
    _print_mock(vif_data.set_index('features').head(max_features_to_display))  # pytype: disable=wrong-arg-types  # pandas-drop-duplicates-overloads
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
