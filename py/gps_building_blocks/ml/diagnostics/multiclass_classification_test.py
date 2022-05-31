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
"""Tests for multiclass_classification."""
from absl.testing import absltest
import numpy as np
import pandas as pd
from absl.testing import parameterized
from gps_building_blocks.ml.diagnostics import multiclass_classification

_TEST_DATA = pd.DataFrame({
    'label': [1, 2, 1, 0, 1, 1, 1, 1, 0, 0, 0, 2, 0, 1, 0, 0, 0, 0],
    'prediction': [1, 0, 0, 0, 2, 1, 0, 1, 0, 1, 0, 2, 0, 1, 1, 0, 0, 0]
})

pred_probs = np.array([[1.21958900e-01, 8.78030305e-01, 1.07949250e-05],
                       [7.97058292e-01, 2.02911413e-01, 3.02949242e-05],
                       [8.51997665e-01, 1.47976480e-01, 2.58550858e-05],
                       [8.23406019e-01, 1.76536159e-01, 5.78217704e-05],
                       [1.11907339e-05, 1.03953836e-01, 8.96034973e-01],
                       [7.37527845e-02, 9.26234254e-01, 1.29612594e-05],
                       [8.94096848e-01, 1.05863935e-01, 3.92166195e-05],
                       [1.39946715e-01, 8.60034410e-01, 1.88751124e-05],
                       [8.01028643e-01, 1.98886755e-01, 8.46025595e-05],
                       [2.07312003e-01, 7.92662392e-01, 2.56051563e-05],
                       [8.90486112e-01, 1.09507726e-01, 6.16178069e-06],
                       [1.38164963e-01, 3.43688884e-05, 8.61800668e-01],
                       [7.85364369e-01, 2.14608265e-01, 2.73660893e-05],
                       [1.66845600e-01, 8.33122325e-01, 3.20746459e-05],
                       [7.28939564e-02, 9.27105079e-01, 9.65025075e-07],
                       [9.64209783e-01, 3.57879589e-02, 2.25815135e-06],
                       [9.40244677e-01, 5.97504819e-02, 4.84124978e-06],
                       [8.90383643e-01, 1.09602199e-01, 1.41584311e-05]])

class_label_names = {0: 'low_value', 1: 'mid_value', 2: 'high_value'}


class MulticlassClassificationTest(parameterized.TestCase, absltest.TestCase):

  def test_calc_performance_metrics_returns_correct_values(self):
    expected_auc = 0.724
    results = multiclass_classification.calc_performance_metrics(
        labels=_TEST_DATA['label'].values,
        predictions=_TEST_DATA['prediction'].values,
        pred_probs=pred_probs,
        class_label_names=class_label_names)
    auc_roc_score = results['auc_roc_score']
    self.assertAlmostEqual(auc_roc_score, expected_auc)


if __name__ == '__main__':
  absltest.main()
