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

"""Tests for tcrm.utils.errors."""

import unittest

from gps_building_blocks.tcrm.utils import errors


class ErrorsTest(unittest.TestCase):

  def test_error_class_repr_prints_class_only(self):
    error = errors.Error()

    self.assertEqual(str(error), 'Error %d - %s' % (
        errors.ErrorNameIDMap.ERROR.value, type(error).__name__))

  def test_error_class_repr_prints_msg(self):
    error_msg = 'Error message.'
    error = errors.Error(msg=error_msg)

    self.assertEqual(str(error), 'Error %d - %s: %s' % (
        errors.ErrorNameIDMap.ERROR.value, type(error).__name__, error_msg))

  def test_error_class_repr_prints_given_error(self):
    base_error = Exception('Base error message.')
    error = errors.Error(error=base_error)

    self.assertEqual(str(error), 'Error %d - %s\nSee causing error:\n%s' % (
        errors.ErrorNameIDMap.ERROR.value,
        type(error).__name__, str(base_error)))

  def test_error_class_repr_prints_msg_and_given_error(self):
    error_msg = 'Error message.'
    base_error = Exception('Base error message.')
    error = errors.Error(msg=error_msg, error=base_error)

    self.assertEqual(str(error), 'Error %d - %s: %s\nSee causing error:\n%s' % (
        errors.ErrorNameIDMap.ERROR.value,
        type(error).__name__, error_msg, str(base_error)))

  def test_errors_map_and_description_map_match(self):
    for error_num in errors.ErrorNameIDMap._member_map_:
      self.assertIn(errors.ErrorNameIDMap[error_num].value,
                    errors._ERROR_ID_DESCRIPTION_MAP)

    self.assertEqual(len(errors.ErrorNameIDMap._member_map_),
                     len(errors._ERROR_ID_DESCRIPTION_MAP))

if __name__ == '__main__':
  unittest.main()
