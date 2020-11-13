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

"""Functions commonly used to perform feature enineering.
"""

import pandas as pd
from statsmodels.stats.outliers_influence import variance_inflation_factor
from statsmodels.tools.tools import add_constant


def calculate_vif(data: pd.DataFrame, sort: bool = True) -> pd.DataFrame:
  """Calculates Variance Inflation Factors (VIFs) of a pandas dataframe.

  VIFs are a statistical measure of multicolinearity between a set of variables.
  See https://en.wikipedia.org/wiki/Variance_inflation_factor.

  Args:
    data: Must not include the response variable (Y). Must be numeric (no
    strings or categories).
    sort: If True, sorts the results by the VIFs in descending order.

  Returns:
    A VIF value for each feature.
  """

  assert all([pd.api.types.is_numeric_dtype(data[col]) for col in data.columns
             ]), ('All columns must be numeric. Try one hot encoding the data.')

  # Expects an intercept column to give the correct results.
  data = add_constant(data, has_constant='skip')

  vif_list = []
  for i in range(data.shape[1]):
    vif_list.append(variance_inflation_factor(data.values, i))

  vif_df = pd.DataFrame({'VIF': vif_list, 'features': data.columns})

  # We have to remove the constant.
  vif_df = vif_df.loc[vif_df['features'] != 'const']

  if sort:
    vif_df = vif_df.sort_values('VIF', ascending=False)
  return vif_df
