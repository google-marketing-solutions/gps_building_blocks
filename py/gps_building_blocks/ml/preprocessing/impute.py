# Copyright 2022 Google LLC
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
"""Imputation for missing data of different types.

Impute utilises an iterative imputation approach as well as LightGBM
(for categorical data) to impute missing values. In many cases, imputing missing
values might be preferrable over dropping the whole observation, as this might
introduce bias.

For most users, the following flow would allow to impute missing categorical and
numerical data:

'''
data_types = detect_data_types(data)
for column, data_type in zip(data.columns, data_types):
  if data_type == 'categorical':
    data[column], _ = encode_categorical_data(
    data, data_types)
data, _ = impute_numerical_data(data, data_types, impute.IterativeImputer())
'''

As a one-liner, users can also run:
imputed_data = run_imputation_pipeline(data, categorical_cutoff=10, max_iter=
10, random_state=10)
"""


import dataclasses
from typing import Dict, List, Optional, Sequence, Tuple, Union

import lightgbm as lgb
import numpy as np
import pandas as pd
from scipy import special
from sklearn import base
from sklearn import impute
from sklearn import metrics
from sklearn import model_selection
from sklearn import preprocessing
from sklearn.experimental import enable_iterative_imputer  # pylint:disable=unused-import

SUPPORTED_DATATYPES = ('binary', 'categorical', 'numerical')


@dataclasses.dataclass(frozen=True)
class ImputationPerformance:
  performance_imputed: float
  performance_missing: float


def detect_data_types(data: pd.DataFrame,
                      categorical_cutoff: int = 10,
                      ignore_nans: bool = True) -> List[str]:
  """Detects type of input data, ignoring NaNs.

  Args:
    data: Data of which types needs to be detected.
    categorical_cutoff: Cut-off to decide whether a variable is treated as
      numerical/continuous or categorical.
    ignore_nans: Whether or not NaNs should be ignored when counting unique
      values. If true, [NaN, 0, 1] is considered having 2 unique values.

  Returns:
    Data type: One of numerical (int or float), categorical, or binary.

  Raises:
    ValueError if the categorical cut-off is set to a value <3.
  """

  if categorical_cutoff < 3:
    raise ValueError(
        'Variables with only 2 expressions should be encoded as binary.',
        'Please adjust your cut-off value.')
  data_types = []
  # TODO()
  for feature in data.columns:
    if data[feature].dtype == 'object':
      data_types.append('categorical')
    elif data[feature].dropna().nunique(
    ) == 2 if ignore_nans else data[feature].nunique() == 2:
      data_types.append('binary')
    elif data[feature].dropna(
    ).nunique() <= categorical_cutoff if ignore_nans else data[feature].nunique(
    ) <= categorical_cutoff:
      data_types.append('categorical')
    else:
      data_types.append('numerical')  # TODO()
  return data_types


def _retrieve_categorical_and_numerical_or_binary_columns(
    data: pd.DataFrame, data_types: Sequence[str]
) -> Tuple[List[str], List[str]]:
  """Returns categorical and numerical columns in dataframe based on data types.
  """
  if not set(data_types).issubset(SUPPORTED_DATATYPES):
    raise ValueError(
        f'Only {SUPPORTED_DATATYPES} are supported for imputation.'
    )
  numerical_binary_columns = [
      column
      for column, data_type in zip(data.columns, data_types)
      if data_type in ('binary', 'numerical')
  ]
  categorical_columns = [
      column
      for column, data_type in zip(data.columns, data_types)
      if data_type == 'categorical'
  ]
  return categorical_columns, numerical_binary_columns


def encode_categorical_data(
    data: pd.DataFrame, data_types: Sequence[str]
) -> Tuple[pd.DataFrame, preprocessing.OrdinalEncoder]:
  """Encodes identified categorical data as (numerical) categories in dataframe.

  Categorical data can be directly declared as 'categorical' in a dataframe,
  which allows us to use it without one-hot encoding in classification models
  like lGBM.

  Args:
    data: All data for which a data type has been identified.
    data_types: Types of the data, for instance numerical or categorical.

  Returns:
    Data with categorical encoding for categorical variables, as well as the fit
    encoder for later reversal of the encoding.
  """
  categorical_columns, _ = (
      _retrieve_categorical_and_numerical_or_binary_columns(data, data_types)
  )
  encoded_data = data.copy()
  ordinal_encoder = preprocessing.OrdinalEncoder()
  encoded_data[categorical_columns] = ordinal_encoder.fit_transform(
      encoded_data[categorical_columns]
  )
  encoded_data[categorical_columns] = encoded_data[categorical_columns].astype(
      'category'
  )
  return encoded_data, ordinal_encoder


def impute_categorical_data(
    data: pd.DataFrame,
    target: pd.Series,
    data_types: Sequence[str],
    random_state: Optional[int] = None,
) -> Tuple[pd.Series, pd.Series]:
  """Uses LightGBM classification to impute categorical missing data.

  The goal here is to impute missing data in our target variable. LightGBM can
  handle missing data in the features, which makes it our model of choice.
  It is assumed that the target variable is categorical. Categorical features in
  the data will be automatically handled appropriately by the LightGBM model,
  but need to be correctly declared in data_types.

  Args:
    data: Input data including all features for which a feature-type has been
      identified.
    target: Target variable for which missing features should be imputed.
    data_types: Data types of all features.
    random_state: Random state to use for reproducible model fitting.

  Returns:
    Data with imputed values and positions of originally missing values.
  """
  missing_indices = target.isna()
  if missing_indices.sum() == 0:
    return target, missing_indices
  categorical_columns, _ = (
      _retrieve_categorical_and_numerical_or_binary_columns(data, data_types)
  )
  categorical_columns.remove(target.name)
  model = lgb.LGBMClassifier(use_missing=True, random_state=random_state)
  features = data.drop(labels=[target.name], axis=1)
  model.fit(
      features[~missing_indices],
      target[~missing_indices],
      categorical_feature=categorical_columns)
  predicted_data = model.predict(features[missing_indices])
  target.loc[missing_indices.values] = predicted_data
  return target, missing_indices


def _one_hot_encode(
    data: np.ndarray
) -> Tuple[np.ndarray, preprocessing._encoders.OneHotEncoder, int]:
  """Applies one-hot encoding to categorical data."""
  one_hot_encoder = preprocessing.OneHotEncoder()
  one_hot_encoded_data = one_hot_encoder.fit_transform(data).toarray()
  index_numerical_features = np.shape(one_hot_encoded_data)[1]
  return (one_hot_encoded_data, one_hot_encoder, index_numerical_features)


def _reverse_one_hot_encoding(
    data: np.ndarray, one_hot_encoder: preprocessing._encoders.OneHotEncoder,
    index_numerical_features: int) -> np.ndarray:
  """Reverses one-hot encoding of categorical features."""
  data_reverse_one_hot = one_hot_encoder.inverse_transform(
      data[:, :index_numerical_features])
  return np.concatenate(
      (data_reverse_one_hot, data[:, index_numerical_features:]), axis=1)


def impute_numerical_data(
    *,
    data: pd.DataFrame,
    data_types: Sequence[str],
    imputer: impute.IterativeImputer,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
  """Uses sklearn's IterativeImputer to impute missing values.

  Missing values in numerical featuers are imputed using the IterativeImputer,
  implemented in sklearn.

  Args:
    data: Input data with missing values.
    data_types: Data types of features in data.
    imputer: Instance of sklearn's IterativeImputer.

  Returns:
    Data with imputed values and mask indicating where values were originally
    missing.

  Raises:
    ValueError if data contains NaNs in categorical columns.
  """

  if data.notna().values.all():
    return (data, data.isna())

  categorical_columns, numerical_columns = (
      _retrieve_categorical_and_numerical_or_binary_columns(data, data_types)
  )

  if categorical_columns:
    if data[categorical_columns].isna().values.any():
      raise ValueError(
          'Categorical columns contain NaNs.'
          'Please run impute_categorical_data first.'
      )

    one_hot_encoded_data, one_hot_encoder, index_numerical_features = (
        _one_hot_encode(data[categorical_columns].values)
    )
    data_all = np.concatenate(
        (one_hot_encoded_data, data[numerical_columns].values), axis=1
    )
    data_imputed_one_hot = imputer.fit_transform(data_all)
    data_imputed = _reverse_one_hot_encoding(
        data_imputed_one_hot, one_hot_encoder, index_numerical_features
    )
  else:
    data_imputed = imputer.fit_transform(data)
  return (
      pd.DataFrame(
          data_imputed,
          columns=np.concatenate((categorical_columns, numerical_columns)),
      ),
      data.isna(),
  )


def post_process_binary_data(data: pd.Series):
  """Rounds imputed data in binary columns to be either 0 or 1."""
  return data.apply(np.round).apply(np.clip, a_min=0, a_max=1)


def run_imputation_pipeline(
    data: pd.DataFrame,
    categorical_cutoff: int = 10,
    data_types: Sequence[str] = (),
    scaling: bool = True,
    max_iter: int = 10,
    random_state: Optional[int] = None,
    parameters_iterativeimputer: Optional[Dict[str, Union[int, str]]] = None,
) -> pd.DataFrame:
  """Runs the full imputation pipeline.

    This function runs the full pipeline to impute missing data.

  Args:
    data: Data with missings encoded as NaNs to impute.
    categorical_cutoff: Cutoff-value to use when deciding if numerical data
      should be treated as categorical or continuous.
    data_types: Data types present in the data. If this is not provided, types
      are automatically detected.
    scaling: Whether or not numerical data (continuous and binary) should be
      scaled to 0/1 before imputation.
    max_iter: Maximum number of iterations used by sklearn's IterativeImputer.
    random_state: Random state to use for IterativeImputer.
    parameters_iterativeimputer: additional parameters to pass to
      IterativeImputer.

  Raises:
    ValueError, if the length of provided data types doesn't match the number
    of columns.
  Returns:
    Data with imputed values.
  """
  if data_types:
    if len(data_types) != len(data.columns):
      raise ValueError(
          'Number of passed data types is not equal to the number '
          'of columns in the data. Please provide one type per '
          'column or pass None to detect data types automatically.'
      )

  if not parameters_iterativeimputer:
    parameters_iterativeimputer = {}
  imputer = impute.IterativeImputer(
      random_state=random_state,
      max_iter=max_iter,
      **parameters_iterativeimputer,
  )
  if not data_types:
    data_types = detect_data_types(data, categorical_cutoff=categorical_cutoff)
  categorical_columns, numerical_columns = (
      _retrieve_categorical_and_numerical_or_binary_columns(data, data_types)
  )
  data_imputed = data.copy()
  if scaling:
    scaler = preprocessing.MinMaxScaler(feature_range=(0, 1))
    data_imputed[numerical_columns], _ = scaler.fit_transform(
        data_imputed[numerical_columns]
    )
  for column in categorical_columns:
    data_imputed[column] = impute_categorical_data(
        data, data[column], data_types, random_state
    )
  data_imputed, _ = impute_numerical_data(
      data=data_imputed, data_types=data_types, imputer=imputer
  )
  for column, data_type in zip(data.columns, data_types):
    if data_type == 'binary':
      data_imputed[column] = post_process_binary_data(data_imputed[column])
  if scaling:
    data_imputed[numerical_columns] = scaler.inverse_transform(
        data_imputed[numerical_columns]
    )
  return data_imputed


# simulation functions
def _generate_categorical_variable(
    n_samples: int,
    hidden_variable: Sequence[float],
    categorical_variables: Sequence[str] = ('a', 'b', 'c'),
    probabilities: Sequence[float] = (0.7, 0.2, 0.1),
) -> np.ndarray:
  """Returns an array of categorical variables, depending on a hidden variable.

  Draws categorical variables from two different underlying distributions,
  depending on the value of a hidden variable.

  Args:
    n_samples: Number of samples.
    hidden_variable: Vector of hidden variable which determines probability
      distribution to draw from.
    categorical_variables: Categorical variables to generate.
    probabilities: Probability distribution underlying categorical variables.

  Returns:
    Random samples of categorical variables.
  """

  if len(categorical_variables) != len(probabilities):
    raise ValueError(
        'Lenght of arrays of categorical variables and '
        'corresponding probabilities does not match.'
    )

  categorical_variable = []
  for i in range(n_samples):
    if hidden_variable[i] > 0:
      categorical_variable.append(
          np.random.choice(categorical_variables, p=probabilities)
      )
    else:
      categorical_variable.append(
          np.random.choice(categorical_variables, p=probabilities[::-1])
      )
  return np.array(categorical_variable)


def _generate_missing_data(
    data: pd.DataFrame, rate_missings: float
) -> pd.DataFrame:
  """Replaces data entries with nans to create missing data.

  Args:
    data: Ground-truth data of which missings should be generated.
    rate_missings: Rate of missing data to be generated.

  Returns:
    Data with missing values.
  """

  data_missing = data.copy()
  n = len(data)
  for variable in list(data_missing):
    missing_index = np.random.choice(n, int(rate_missings * n), replace=False)
    data_missing[variable][data_missing.index.isin(missing_index)] = np.nan
  return data_missing


def simulate_mixed_data_with_missings(
    n_samples: int = 1000,
    n_categorical: int = 1,
    n_continuous: int = 1,
    n_binary: int = 1,
    rate_missings: float = 0.1,
    random_seed: Optional[int] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
  """Generates observations of correlated variables with missing values.

  Args:
    n_samples: Number of samples to simulate.
    n_categorical: Number of categorical variables to simulate.
    n_continuous: Number of continuous variables to simulate.
    n_binary: Number of binary variables to simulate.
    rate_missings: Rate of missing data.
    random_seed: Random seed for reproducible results.

  Returns:
   Simulated data with and without missings.

  Raises:
    ValueError, if the missing rate is not in the range of 0-1.
  """

  if 0 >= rate_missings >= 1:
    raise ValueError('Missing-rate needs to be between 0 and 1.')

  if random_seed:
    np.random.seed(random_seed)
  hidden_variable = np.random.normal(0, 1, n_samples)
  data_dict = {}
  variable_index = 0
  for _ in range(n_categorical):
    data_dict[variable_index] = _generate_categorical_variable(
        n_samples, hidden_variable
    )
    variable_index += 1
  for _ in range(n_continuous):
    data_dict[variable_index] = hidden_variable + np.random.normal(
        0, 1, n_samples
    )
    variable_index += 1
  for _ in range(n_binary):
    data_dict[variable_index] = np.random.binomial(
        1, special.expit(hidden_variable), n_samples
    ).astype(np.int32)
    variable_index += 1

  data = pd.DataFrame(data_dict)
  data_missing = _generate_missing_data(data, rate_missings)
  return data, data_missing


# evaluation functions
def calculate_model_performance_gain_from_imputation(
    *,
    model: base.BaseEstimator,
    data_imputed: pd.DataFrame,
    data_missings: pd.DataFrame,
    target: pd.Series,
    metric: str | None = None,
    crossvalidation_folds: int | None = None,
    drop_missings: bool | None = True,
) -> ImputationPerformance:
  """Runs crossvalidation to estimate model performance gain of imputation.

  After running the data imputation, this function evaluates which performance
  gain a chosen model would get from imputing missing values compared to
  dropping all rows with missing values.
  As a caveat, it should be noted that this means that not only more data is
  for training the models, but also for calculating their performance.

  Args:
    model: Model instance compatible with sklearn's cross_val_score to train on
      the present data.
    data_imputed: Data with imputed missings. Only numerical data is supported.
    data_missings: Original dataset with missings coded as NaN. Only numerical
      data is supported.
    target: Target column, including missing data points.
    metric: Chosen metric for model evaluation. Needs to be compatible with
      sklearn's cross_val_score method.
    crossvalidation_folds: Number of folds to use for cross-validation.
    drop_missings: Whether or not to drop missing values. If the chosen model
      can cope with missings (e.g., LightGBM), model performance can still be
      assessed.

  Returns:
    Model performance for imputed and original dataset.

  Raises:
    ValueError if non-numerical data is detected, or if not enough valid data
    points are available for model fitting.
  """

  if (
      any(data_imputed.dtypes == 'object')
      or any(data_imputed.dtypes == 'datetime')
      or any(data_missings.dtypes == 'object')
      or any(data_missings.dtypes == 'datetime')
  ):
    invalid_columns = []
    if any(data_imputed.dtypes == 'object') or any(
        data_imputed.dtypes == 'datetime'
    ):
      invalid_columns.extend(
          [
              col
              for col, dtype in zip(data_imputed.columns, data_imputed.dtypes)
              if dtype == 'object' or dtype == 'datetime'
          ]
      )

    if any(data_missings.dtypes == 'object') or any(
        data_missings.dtypes == 'datetime'
    ):
      invalid_columns.extend(
          [
              col
              for col, dtype in zip(data_missings.columns, data_missings.dtypes)
              if dtype == 'object' or dtype == 'datetime'
          ]
      )

    raise ValueError(
        'Only numerical datatypes are supported by this function, but the '
        f'columns {invalid_columns} are objects or datetimes. ',
        'Please convert your data using e.g. one-hot encoding.',
    )

  non_missing_target_indices = target.notna()

  imputed_performance = model_selection.cross_val_score(
      estimator=model,
      X=data_imputed[non_missing_target_indices],
      y=target[non_missing_target_indices],
      cv=crossvalidation_folds,
      scoring=metric,
  )

  if drop_missings:
    indices_missing_data = data_missings.isna().any(axis=1)
    data_missings_valid = data_missings[
        non_missing_target_indices & ~indices_missing_data
    ]
    target_only_valid = target[
        non_missing_target_indices & ~indices_missing_data
    ]
  else:
    data_missings_valid = data_missings[non_missing_target_indices]
    target_only_valid = target[non_missing_target_indices]

  print(len(data_missings_valid))
  if len(data_missings_valid) <= 1:
    raise ValueError(
        'Not enough observation without missing data, '
        'model fitting is not possible.'
    )

  missings_performance = model_selection.cross_val_score(
      estimator=model,
      X=data_missings_valid,
      y=target_only_valid,
      cv=crossvalidation_folds,
      scoring=metric,
  )
  return ImputationPerformance(
      performance_imputed=np.mean(imputed_performance),
      performance_missing=np.mean(missings_performance),
  )


def get_imputation_mean_absolute_error(
    data_ground_truth: pd.DataFrame,
    data_missing: pd.DataFrame,
    data_imputed: pd.DataFrame,
    data_types: Sequence[str],
) -> Sequence[float]:
  """Calculates the mean absolute errors between ground-truth and imputed data.

  If the ground-truth data is known, this function calculates the MAE between
  the imputed and ground-truth data. Only data points for which data was imputed
  are being considered in the calculation. Categorical columns are not included
  in the calculation.

  Args:
    data_ground_truth: Ground-truth data, for instance from a simulation.
    data_missing: Data with missing entries.
    data_imputed: Data with imputed entries.
    data_types: Data types of the three input data.

  Returns:
    Mean Absolute Error for imputed datapoints.

  Raises:
    ValueError if the ground-truth or imputed data contains NaNs, or if no
    numerical column contains missing data.
  """

  if data_ground_truth.isna().values.any():
    raise ValueError('The ground-truth data should not contain NaNs.')

  if data_imputed.isna().values.any():
    raise ValueError('The imputed data should not contain NaNs.')

  _, numerical_columns = _retrieve_categorical_and_numerical_or_binary_columns(
      data_ground_truth, data_types
  )
  if not data_missing[numerical_columns].isna().values.any():
    raise ValueError('No missing data in any numerical columns.')

  imputation_mean_absolute_error = []
  for column in numerical_columns:
    missing_index = data_missing[column].isna()
    imputation_mean_absolute_error.append(
        metrics.mean_absolute_error(
            data_ground_truth[column][missing_index],
            data_imputed[column][missing_index],
        )
    )
  return imputation_mean_absolute_error
