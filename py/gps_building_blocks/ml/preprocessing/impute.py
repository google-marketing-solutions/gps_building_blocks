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

import pandas as pd
from sklearn import preprocessing


def detect_data_types(data: pd.DataFrame,
                      categorical_cutoff: int = 10) -> List[str]:
  """Detects type of input data.

  Args:
    data: Data of which types needs to be detected.
    categorical_cutoff: Cut-off to decide whether a variable is treated as
      numerical/continuous or categorical.

  Returns:
    Data type: One of numerical (int or float), categorical, or binary.

  Raises:
    ValueError if the categorical cut-off is set to a value <3.
  """

  if categorical_cutoff < 3:
    raise ValueError(
        'Variables with only 2 expressions should be encoded as binary.'
        'Please adjust your cut-off value.')
  data_types = []
  # TODO()
  for feature in data.columns:
    if data[feature].dtype == 'object':
      data_types.append('categorical')
    elif data[feature].nunique() == 2:
      data_types.append('binary')
    elif data[feature].nunique() <= categorical_cutoff:
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
