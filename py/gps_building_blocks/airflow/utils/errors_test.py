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

"""Tests for airflow.utils.errors."""

from absl.testing import absltest

from gps_building_blocks.airflow.utils import errors


class ErrorsTest(absltest.TestCase):

  def test_error_class_repr_prints_class_only(self):
    error = errors.Error()

    self.assertEqual(str(error), '%s' % type(error).__name__)

  def test_error_class_repr_prints_msg(self):
    error_msg = 'Error message.'
    error = errors.Error(msg=error_msg)

    self.assertEqual(str(error), '%s %s' % (type(error).__name__, error_msg))

  def test_error_class_repr_prints_given_error(self):
    base_error = Exception('Base error message.')
    error = errors.Error(error=base_error)

    self.assertEqual(str(error),
                     '%s: %s' % (type(error).__name__, str(base_error)))

  def test_error_class_repr_prints_msg_and_given_error(self):
    error_msg = 'Error message.'
    base_error = Exception('Base error message.')
    error = errors.Error(msg=error_msg, error=base_error)

    self.assertEqual(str(error),
                     '%s %s: %s' % (type(error).__name__,
                                    error_msg, str(base_error)))

if __name__ == '__main__':
  absltest.main()
