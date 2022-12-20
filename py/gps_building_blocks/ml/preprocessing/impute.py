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

Impute utilises an iterative imputation approach as well as logistic regression
(for categorical data) to impute missing values. In many cases, imputing missing
values might be preferrable over dropping the whole observation, as this might
introduce bias.

For most users, the following flow would allow to impute missing categorical and
numerical data:

'''
data_types = detect_data_type(data)
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


from typing import List, Sequence, Tuple

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn import impute
from sklearn import preprocessing
from sklearn.experimental import enable_iterative_imputer  # pylint:disable=unused-import


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
  categorical_columns = [
      column for column, data_type in zip(data.columns, data_types)
      if data_type == 'categorical'
  ]
  ordinal_encoder = preprocessing.OrdinalEncoder()
  data[categorical_columns] = ordinal_encoder.fit_transform(
      data[categorical_columns])
  data[categorical_columns] = data[categorical_columns].astype('category')
  return data, ordinal_encoder


def impute_categorical_data(
    data: pd.DataFrame, target: pd.Series,
    data_types: Sequence[str]) -> Tuple[pd.Series, pd.Series]:
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

  Returns:
    Data with imputed values and positions of originally missing values.
  """
  missing_indices = target.isna()
  if missing_indices.sum() == 0:
    return target, missing_indices
  categorical_columns = [
      column for column, data_type in zip(data.columns, data_types)
      if data_type == 'categorical'
  ]
  categorical_columns.remove(target.name)
  model = lgb.LGBMClassifier(use_missing=True)
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
    data: pd.DataFrame, data_types: Sequence[str],
    imputer: impute.IterativeImputer) -> Tuple[pd.DataFrame, pd.DataFrame]:
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
    return data

  non_numerical_columns = [
      column for column, data_type in zip(data.columns, data_types)
      if data_type == 'categorical'
  ]
  numerical_columns = [
      column for column, data_type in zip(data.columns, data_types)
      if data_type != 'categorical'
  ]

  if data[non_numerical_columns].isna().values.any():
    raise ValueError('Categorical columns contain NaNs.'
                     'Please run impute_categorical_data first.')
  one_hot_encoded_data, one_hot_encoder, index_numerical_features = _one_hot_encode(
      data[non_numerical_columns].values)
  data_all = (
      np.concatenate((one_hot_encoded_data, data[numerical_columns].values),
                     axis=1))
  data_imputed_one_hot = imputer.fit_transform(data_all)
  data_imputed = _reverse_one_hot_encoding(data_imputed_one_hot,
                                           one_hot_encoder,
                                           index_numerical_features)
  return pd.DataFrame(
      data_imputed,
      columns=np.concatenate(
          (non_numerical_columns, numerical_columns))), data.isna()
