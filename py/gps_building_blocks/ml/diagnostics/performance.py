
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
"""Set of recurring functions used to diagnose and debug model performance."""
from typing import Tuple

import numpy as np
import pandas as pd
import seaborn as sns


def _compute_quantile_accuracies(heatmap: pd.DataFrame) -> Tuple[float, float]:
  """Computes the accuracy within 1st and 2nd quantile."""
  # TODO(): Add overall accuracy result.
  # Create filters to calculate accuracy within 1st and 2nd quantile.
  mask_1st_quantile = (
      np.eye(*heatmap.shape) +
      np.eye(*heatmap.shape, k=1) +
      np.eye(*heatmap.shape, k=-1))
  mask_2nd_quantile = (
      mask_1st_quantile +
      np.eye(*heatmap.shape, k=2) +
      np.eye(*heatmap.shape, k=-2))
  # Calculate accuracy.
  all_sum = heatmap.sum().sum()
  accuracy_1st_quantile = (heatmap * mask_1st_quantile).sum().sum() / all_sum
  accuracy_2nd_quantile = (heatmap * mask_2nd_quantile).sum().sum() / all_sum
  return accuracy_1st_quantile, accuracy_2nd_quantile


def plot_quantile_accuracy_heatmap(
    actual: pd.Series,
    prediction: pd.Series,
    bins: int = 10,
    normalize: bool = True,
    color_map: str = 'Oranges',
    verbose: bool = True) -> None:
  """Plots the accuracy heatmap with binned actuals/predictions into quantiles.

  The accuracy of a regression model can be evaluated by visualizing the heatmap
  of a confusion matrix using the binned values of actual and predicted results.
  This is particulary useful to roughly understand how accurate your model is
  rather than relying on purerly numeric regressions performance metrics (eg.
  RMSE, MAE or MAPE).

  Args:
    actual: Series with the actual values.
    prediction: Series with prediction values.
    bins: Number of quantile bins to create.
    normalize: Normalizes the heatmap value with percentages. The percentages in
      each block of the heatmap, represent the accuracy of the model within the
      same quantile (each row will add-up to one).
    color_map: Matplotlib colormap to use in the heatmap. Full list available at
      https://matplotlib.org/3.1.0/tutorials/colors/colormaps.html.
    verbose: If True, will print the accuracy of the 1st and 2nd bins.
  """
  actual = actual.rename('actual')
  prediction = prediction.rename('prediction')

  heatmap = pd.crosstab(
      index=pd.qcut(prediction, bins, labels=False) + 1,
      columns=pd.qcut(actual, bins, labels=False) + 1,
      normalize='index' if normalize else False).fillna(0)

  sns.heatmap(heatmap.round(2), annot=True, fmt='g', cmap=color_map)

  if verbose:
    accuracy_1st_quantile, accuracy_2nd_quantile = _compute_quantile_accuracies(
        heatmap)
    print(f'{accuracy_1st_quantile*100:.2f}% accuracy within 1st quantile')
    print(f'{accuracy_2nd_quantile*100:.2f}% accuracy within 2nd quantile')
