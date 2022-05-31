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
"""Produces plots and statistics to diagnose a multiclass classification model.

Specially useful when diagnosing a Lifetime Value (LTV) model.
"""
from typing import Dict, Optional, Union

from matplotlib import pyplot as plt
import numpy as np
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
from sklearn.metrics import ConfusionMatrixDisplay
from sklearn.metrics import roc_auc_score

from gps_building_blocks.ml import utils


def calc_performance_metrics(
    labels: np.ndarray,
    predictions: np.ndarray,
    pred_probs: np.ndarray,
    class_label_names: Optional[Dict[Union[str, int], Union[str, int]]] = None,
    decimal_points: Optional[int] = 3,
    average_type: Optional[str] = 'weighted',
    multi_class_type: Optional[str] = 'ovr') -> Dict[str, float]:
  """Calculates performance metrics for a multiclass classification model.

  Args:
    labels: An array of true labels containing multiclass labels.
    predictions: An array of predictions containing multiclass labels.
    pred_probs: An array of shape (n_samples, n_classes) of predicted
      probabilities.
    class_label_names: Optional. Dictionary of multiclass labels and
      corresponding target names. The type of both class lable and target names
      can be either 'int' or 'str'. E.g. {0: 'low_value', 1: 'mid_value', 2:
      'high_value'}.
    decimal_points: Number of decimal points to use when outputting the
      calculated evaluation metrics.
    average_type: The averaging method applied to the data while calculating
      scores.
    multi_class_type: The method applied to AUC calculation. It can take 'ovr'
      or 'ovo'. 'ovr' stands for One-vs-rest. 'ovo' stands for One-vs-one.

  Returns:
    Dictionary of evaluation metrics of a multiclass classification model:
    {classification_report: Summary report of precision, recall, F1 score for
    each class.
    auc_roc_score: Area under the recall vs (1-specificity) (ROC) curve.
    confusion_matrix: Confusion matrix to evaluate the accuracy of each class.
  """
  utils.assert_label_and_prediction_length_match(labels, predictions)
  assert len(labels) == pred_probs.shape[0], (
      'The true labels and prediction probability should have the same length.')
  assert len(set(labels)) == pred_probs.shape[1], (
      'The number of classes of labels and prediction probability should be '
      'the same.'
  )
  if class_label_names is None:
    class_labels = list(set(labels))
    target_names = ['%s' % l for l in class_labels]
  else:
    class_labels = list(class_label_names.keys())
    target_names = list(class_label_names.values())

  class_report = classification_report(
      y_true=labels,
      y_pred=predictions,
      labels=class_labels,
      target_names=target_names)
  auc_score = roc_auc_score(
      y_true=labels,
      y_score=pred_probs,
      average=average_type,
      multi_class=multi_class_type)

  conf_matrix = confusion_matrix(
      y_true=labels, y_pred=predictions, labels=class_labels)

  return {
      'classification_report': class_report,
      'auc_roc_score': round(auc_score, decimal_points),
      'confusion_matrix': conf_matrix
  }


def plot_confusion_matrix(labels: np.ndarray,
                          predictions: np.ndarray,
                          class_label_names: Optional[Dict[Union[str, int],
                                                           Union[str,
                                                                 int]]] = None,
                          normalize: Optional[str] = None,
                          title_fontsize: Optional[int] = 12,
                          x_label_fontsize: Optional[int] = 12,
                          y_label_fontsize: Optional[int] = 12,
                          heatmap_color: Optional[str] = 'Greens') -> None:
  """Plot confusion matrix for a multiclass classification model.

  Args:
    labels: An array of true labels containing multiclass labels.
    predictions: An array of predictions containing multiclass labels.
    class_label_names: Dictionary of multiclass labels and corresponding target
      names. The type of both class lable and target names can be either 'int'
      or 'str'. E.g. {0: 'low_value', 1: 'mid_value', 2: 'high_value'}.
    normalize: A parameter controlling whether to normalize the counts in the
      matrix.
    title_fontsize: Font size of the figure title.
    x_label_fontsize: Font size of the x axis labels.
    y_label_fontsize: Font size of the y axis labels.
    heatmap_color: Color of the heatmap plot.

  Returns:
    Heatmap of confusion matrix.
  """
  utils.assert_label_and_prediction_length_match(labels, predictions)

  if class_label_names is None:
    class_labels = list(set(labels))
    target_names = ['%s' % l for l in class_labels]
  else:
    class_labels = list(class_label_names.keys())
    target_names = list(class_label_names.values())

  plot = ConfusionMatrixDisplay.from_predictions(
      y_true=labels,
      y_pred=predictions,
      labels=np.unique(labels),
      display_labels=target_names,
      normalize=normalize,
      include_values=True,
      cmap=heatmap_color)
  plot.ax_.set_title('Confusion matrix', fontsize=title_fontsize)
  plot.ax_.set_xlabel('Predicted label', fontsize=x_label_fontsize)
  plot.ax_.set_ylabel('Actual label', fontsize=y_label_fontsize)
  plt.show()
