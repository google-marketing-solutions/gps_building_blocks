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
"""Tests for impute module."""

import pandas as pd
from pandas import testing

from absl.testing import absltest
from gps_building_blocks.ml.preprocessing import impute

data_dict = {
    'cat': list('abcdefg'),
    'num': [1, 2, 3, 4, 5, 6, 7],
    'binary': [0, 0, 0, 1, 1, 1, 1]
}
_MOCK_DATA = pd.DataFrame(data_dict)
_expected_data_types = ['categorical', 'numerical', 'binary']


class ImputeTest(absltest.TestCase):

  def test_detects_expected_data_types(self):
    detected_data_types = impute.detect_data_types(
        _MOCK_DATA, categorical_cutoff=3)

    self.assertEqual(_expected_data_types, detected_data_types)

  def test_length_data_types_matches_data(self):
    detected_data_types = impute.detect_data_types(
        _MOCK_DATA, categorical_cutoff=3)

    self.assertLen(detected_data_types, len(_MOCK_DATA.columns))

  def test_numerical_data_remain_same_after_encoding(self):
    detected_data_types = impute.detect_data_types(
        _MOCK_DATA, categorical_cutoff=3)
    encoded_data, _ = impute.encode_categorical_data(_MOCK_DATA,
                                                     detected_data_types)

    testing.assert_frame_equal(encoded_data[['num', 'binary']],
                               _MOCK_DATA[['num', 'binary']])


if __name__ == '__main__':
  absltest.main()
