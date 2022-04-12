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

"""Tests for airflow.utils.retry_utils."""

from airflow import exceptions
from googleapiclient import errors
import parameterized

from absl.testing import absltest
from absl.testing.absltest import mock
from gps_building_blocks.airflow.utils import retry_utils

_NON_RETRIABLE_HTTP_STATUS_CODES = (401,  # Unauthorized
                                    403,  # Forbidden
                                    404,  # Not Found
                                    405,  # Method Not Allowed
                                    410)  # Gone
_BYTES_ERROR_MESSAGE = bytes('[{"error": {"message": "error"}}]', 'utf-8')


def parameterize_function_name(testcase_func, unused_param_num, param):
  """A helper function to parameterizing a given function name.

  Args:
    testcase_func: The function to parameterize its name.
    unused_param_num: Number of parameters in param (unused in this function).
    param: The parameters to add to the function name

  Returns:
    The new function name with parameters in it.
  """
  return '%s_%s' %(testcase_func.__name__,
                   parameterized.parameterized.to_safe_name(
                       '_'.join(str(x) for x in param.args)))


class RetryUtilsTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    retry_utils._RETRY_UTILS_MAX_RETRIES = 3

  @parameterized.parameterized.expand(
      list([x] for x in retry_utils._RETRY_UTILS_RETRIABLE_STATUS_CODES),
      testcase_func_name=parameterize_function_name)
  def test_logged_retry_on_retriable_http_error_retry_on_status(self, status):
    error = errors.HttpError(mock.MagicMock(status=status),
                             _BYTES_ERROR_MESSAGE)
    mocked_decorated_function_that_throws_error = mock.MagicMock(
        side_effect=error)
    decorated_func = retry_utils.logged_retry_on_retriable_http_error(
        mocked_decorated_function_that_throws_error)

    try:
      decorated_func()
    except errors.HttpError:
      pass

    self.assertEqual(mocked_decorated_function_that_throws_error.call_count,
                     retry_utils._RETRY_UTILS_MAX_RETRIES)

  @parameterized.parameterized.expand(
      list([x] for x in _NON_RETRIABLE_HTTP_STATUS_CODES),
      testcase_func_name=parameterize_function_name)
  def test_logged_retry_on_retriable_http_error_no_retry_on_status(self,
                                                                   status):
    error = errors.HttpError(mock.MagicMock(status=status),
                             _BYTES_ERROR_MESSAGE)
    mocked_decorated_function_that_throws_error = mock.MagicMock(
        side_effect=error)
    decorated_func = retry_utils.logged_retry_on_retriable_http_error(
        mocked_decorated_function_that_throws_error)

    try:
      decorated_func()
    except errors.HttpError:
      pass

    mocked_decorated_function_that_throws_error.assert_called_once()

  def test_logged_retry_on_retriable_http_error_reraises_error(self):
    error = errors.HttpError(mock.MagicMock(), _BYTES_ERROR_MESSAGE)
    mocked_decorated_function_that_throws_error = mock.MagicMock(
        side_effect=error)
    decorated_func = retry_utils.logged_retry_on_retriable_http_error(
        mocked_decorated_function_that_throws_error)

    with self.assertRaises(errors.HttpError):
      decorated_func()

  @parameterized.parameterized.expand(
      list([x] for x in retry_utils._RETRY_UTILS_RETRIABLE_STATUS_CODES),
      testcase_func_name=parameterize_function_name)
  def test_logged_retry_on_retriable_http_airflow_exception_retry_on_status(
      self, status):
    error = exceptions.AirflowException(f'{status}:Error')
    mocked_decorated_function_that_throws_error = mock.MagicMock(
        side_effect=error)
    decorated_func = retry_utils \
                     .logged_retry_on_retriable_http_airflow_exception(
                         mocked_decorated_function_that_throws_error)

    try:
      decorated_func()
    except exceptions.AirflowException:
      pass

    self.assertEqual(mocked_decorated_function_that_throws_error.call_count,
                     retry_utils._RETRY_UTILS_MAX_RETRIES)

if __name__ == '__main__':
  absltest.main()
