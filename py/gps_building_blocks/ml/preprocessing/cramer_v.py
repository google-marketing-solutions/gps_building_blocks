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

"""Cramer's V.

Cramer's V measures the bivariate association between nominal or categorical
variables. This should be done before your run your classifier. This is
for instance used in AutoML to judge good features.

https://en.wikipedia.org/wiki/Cram%C3%A9r%27s_V

https://cloud.google.com/automl/docs/reference/rpc/google.cloud.automl.v1beta1#google.cloud.automl.v1beta1.CorrelationStats

"""

import numpy as np
import pandas as pd
import scipy.stats


def cramer_v(feature: np.ndarray, label: np.ndarray) -> float:
  """Calculates Cramer's V for two categorical variables.

  `feature` and `label` can be interchanged.

  Args:
    feature: a categorical array, can be numeric or string.
    label: a categorical array, can be numeric or string.

  Returns:
    Cramer's V, a float between 0 and 1.

  Raises:
    AssertionError when inputs have wrong length or only distinct values.
  """
  if len(feature) <= 1:
    raise AssertionError('feature needs at least 2 values.')
  if len(feature) != len(label):
    raise AssertionError('feature and label need to have the same length.')
  # If all values in feature/label are distinct the measure is meaningless.
  if len(set(feature)) == len(feature):
    raise AssertionError('feature contains only distinct values.')
  if len(set(label)) == len(label):
    raise AssertionError('label contains only distinct values.')
  confusion_matrix = pd.crosstab(feature, label)
  # Note that there is a change in scipy v1.7. See b/191963980.
  chi2 = scipy.stats.chi2_contingency(confusion_matrix)[0]
  categories_feature, categories_label = len(set(feature)), len(set(label))
  number_obs = len(feature)
  # This uses the formula without bias correction.
  return np.sqrt((chi2 / number_obs) / min(categories_feature - 1,
                                           categories_label - 1))

