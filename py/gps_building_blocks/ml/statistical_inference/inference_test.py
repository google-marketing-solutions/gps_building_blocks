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

"""Tests for gps_building_blocks.ml.statistical_inference.inference."""

import numpy as np
import pandas as pd

from gps_building_blocks.ml.statistical_inference import inference
import unittest


class InferenceTest(googletest.TestCase):
  _missing_data = pd.DataFrame(
      data=[[np.nan, 0.0184],
            [0.5415, 0.0531],
            [0.4161, 1.9822],
            [0.2231, 9.5019]],
      columns=['first', 'second'])

  def test_missing_value_emits_warning_twice(self):
    with self.assertWarns(inference.MissingValueWarning):
      inference.InferenceData(self._missing_data)
    with self.assertWarns(inference.MissingValueWarning):
      inference.InferenceData(self._missing_data)

  def test_check_data_raises_exception_on_missing_data(self):
    inference_data = inference.InferenceData(self._missing_data)

    with self.assertRaises(inference.MissingValueError):
      inference_data.data_check(raise_on_error=True)

  def test_invalid_target_column_raise_exception(self):
    with self.assertRaises(KeyError):
      inference.InferenceData(
          initial_data=self._missing_data,
          target_column='non_ci_sono')


if __name__ == '__main__':
  googletest.main()
