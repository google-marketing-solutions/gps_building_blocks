# python3
# coding=utf-8
# Copyright 2020 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for tcrm.operators.error_report_operator."""

import unittest

from gps_building_blocks.tcrm.operators import error_report_operator


class ErrorReportOperatorTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.task = error_report_operator.ErrorReportOperator(
        task_id='configuration_error',
        error=ValueError('test'))

  def test_execute(self):
    with self.assertRaises(ValueError):
      self.task.execute({})


if __name__ == '__main__':
  unittest.main()
