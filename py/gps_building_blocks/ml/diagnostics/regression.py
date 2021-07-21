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
"""Produces plots and statistics to diagnose a regression model.

Specially useful when diagnosing a Lifetime Value (LTV) model.
"""

from typing import Optional
import dataclasses
import numpy as np
import scipy as sp
from sklearn import metrics
from gps_building_blocks.ml import utils


@dataclasses.dataclass
class _PerformanceMetrics:
  """A container class for the following regression diagnostics metrics.

  Mean squared error.
  Root mean squared error.
  Mean squared log error.
  Mean absolute error.
  Mean absolute percentage error.
  R-squared (Coefficient of Determination).
  Pearson correlation between actual and predicted labels.
  """
  mean_squared_error: float
  root_mean_squared_error: float
  mean_squared_log_error: float
  mean_absolute_error: float
  mean_absolute_percentage_error: float
  r_squared: float
  pearson_correlation: float


def calc_performance_metrics(
    labels: np.ndarray,
    predictions: np.ndarray,
    decimal_points: Optional[int] = 4) -> _PerformanceMetrics:
  """Calculates performance metrics related to a regression model.

  Args:
    labels: An array of true labels containing numeric values.
    predictions: An array of predictions containing numeric values.
    decimal_points: Number of decimal points to use when outputting the
      calculated performance metrics.

  Returns:
    Object of _PerformanceMetrics class containing the regression diagnostics
      metrics.
  """
  utils.assert_label_and_prediction_length_match(labels,
                                                 predictions)

  mse = metrics.mean_squared_error(labels, predictions)
  rmse = np.sqrt(mse)
  msle = np.sqrt(metrics.mean_squared_log_error(labels, predictions))
  mae = metrics.mean_absolute_error(labels, predictions)
  mape = metrics.mean_absolute_percentage_error(labels, predictions)
  r2 = metrics.r2_score(labels, predictions)
  corr = sp.stats.pearsonr(labels, predictions)[0]

  return _PerformanceMetrics(
      mean_squared_error=round(mse, decimal_points),
      root_mean_squared_error=round(rmse, decimal_points),
      mean_squared_log_error=round(msle, decimal_points),
      mean_absolute_error=round(mae, decimal_points),
      mean_absolute_percentage_error=round(mape, decimal_points),
      r_squared=round(r2, decimal_points),
      pearson_correlation=round(corr, decimal_points))
