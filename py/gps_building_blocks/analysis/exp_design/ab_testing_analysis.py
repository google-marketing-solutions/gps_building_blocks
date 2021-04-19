# Copyright 2021 Google LLC
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

# python3
"""Contains functions to statistically analyse the results of an A/B test."""


from typing import Sequence
import numpy as np
from statsmodels.stats import proportion


def calc_chisquared_pvalue(group_counts: Sequence[int],
                           converter_counts: Sequence[int]
                           ) -> np.float64:
  """Performs the Chi-squared statistical test of proportions.

  Args:
    group_counts: Sequence of total user counts in test and control groups. Two
      or more groups should be used.
    converter_counts: Sequence of number of converters in each group specified
      in the group_counts.

  Returns:
    p-value from the test.
  """
  assert len(group_counts) >= 2, 'Two or more goups should be used.'
  assert len(group_counts) == len(converter_counts), (
      'group_counts and converter_counts should have the same length.')

  _, p_val, _ = proportion.proportions_chisquare(count=converter_counts,
                                                 nobs=group_counts)
  return p_val
