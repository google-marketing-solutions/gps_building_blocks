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

"""A utility file for all retry related utils.

All retry consts and retry wrapper should be in this file for
consistent use across the component.

Current implementation uses this retry library:
https://github.com/jd/tenacity/tree/master/tenacity

TODO(): Find a better way to represent retriable and non-retraiable http
status codes.

Example usage:

  @logged_retry_on_retriable_http_error()
  def function_to_retry_on_retriable_http_error():
    pass
"""

import functools
import logging
from typing import Callable, TypeVar

from airflow import exceptions
from googleapiclient import errors
import tenacity


_RT = TypeVar('_RT')   # General return variable

_RETRY_UTILS_MAX_RETRIES = 5
_RETRY_UTILS_RETRIABLE_STATUS_CODES = (429,  # Too Many Requests
                                       500,  # Internal Server Error
                                       503)  # Service Unavailable

_LOGGER = logging.getLogger(__name__)


def _is_retriable_http_error(error: errors.HttpError) -> bool:
  """Checks if HttpError is in _RETRY_UTILS_RETRIABLE_STATUS_CODES.

  This function requires HttpError to have a valid response.

  Args:
    error: The http error to check.

  Returns:
    True if HttpError is retriable, otherwise False.
  """
  if ('resp' in error.__dict__ and
      error.__dict__['resp'].status in _RETRY_UTILS_RETRIABLE_STATUS_CODES):
    return True
  return False


def _is_retriable_http_airflow_exception(
    error: exceptions.AirflowException) -> bool:
  """Checks if AirflowException is raised by an http error in specific codes.

  Such AirflowException is thrown in airflow.hooks.http_hook.HttpHook. This
  function requires AirflowException to have an error message with specified
  format. The format is defined in
  airflow.hooks.http_hook.HttpHook.check_response, which is
  {response.status_code}:{response.reason}. The retriable status codes is
  defined in _RETRY_UTILS_RETRIABLE_STATUS_CODES.

  Args:
    error: The airflow exception to check.

  Returns:
    True if AirflowException is raised by a retriable http error, otherwise
    False.
  """
  text = str(error)
  status_code = text.split(':')[0]
  try:
    if int(status_code) in _RETRY_UTILS_RETRIABLE_STATUS_CODES:
      return True
    return False
  except ValueError:
    return False


def logged_retry_on_retriable_exception(
    function: Callable[..., _RT],
    is_retriable: Callable[..., bool]) -> Callable[..., _RT]:
  """Applies a decorator for retrying a function on retriable exception.

  Wraps a retry decorator for common parameters setting across the component.

  The returned decorator will retry the decorated function should it raise a
  retriable error. The function is_retriable determines whether an error is
  retriable or not.

  The decorated function will be retried up to _RETRY_UTILS_MAX_RETRIES times,
  with an exponential backoff strategy for delays between each retry starting
  from _RETRY_UTILS_INITIAL_DELAY_SEC for the first delay.

  Should the maximum retry number be reached, only the error raised during the
  last retry will be raised.

  Each retry will be logged to _LOGGER.

  Args:
    function: The function to decorate.
    is_retriable: The function to determine whether an error is retriable or
    not.

  Returns:
    Decorated function.
  """
  @functools.wraps(function)
  def decorated_function(*args, **kwargs) -> _RT:
    return tenacity.retry(
        retry=tenacity.retry_if_exception(is_retriable),
        stop=tenacity.stop.stop_after_attempt(_RETRY_UTILS_MAX_RETRIES),
        wait=tenacity.wait.wait_exponential(max=_RETRY_UTILS_MAX_RETRIES),
        after=tenacity.after.after_log(_LOGGER, logging.DEBUG),
        reraise=True
        )(function)(*args, **kwargs)

  return decorated_function


def logged_retry_on_retriable_http_error(function: Callable[..., _RT]
                                        ) -> Callable[..., _RT]:
  """Applies a decorator for retrying a function on retriable http error.

  Args:
    function: The function to decorate.

  Returns:
    Decorated function.
  """
  return logged_retry_on_retriable_exception(function, _is_retriable_http_error)


def logged_retry_on_retriable_http_airflow_exception(
    function: Callable[..., _RT]) -> Callable[..., _RT]:
  """Applies a decorator for retrying a function on airflow exception raised by retriable http error.

  Args:
    function: The function to decorate.

  Returns:
    Decorated function.
  """
  return logged_retry_on_retriable_exception(
      function, _is_retriable_http_airflow_exception)
