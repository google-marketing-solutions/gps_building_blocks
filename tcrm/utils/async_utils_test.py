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

"""Tests for gps_building_blocks.tcrm.utils.async_utils."""

import asyncio
import unittest

from gps_building_blocks.tcrm.utils import async_utils


_BATCH_DATA = ('a', 'b', 'c', 'd', 'e')
_INDEXED_BATCH_RESULT = ({'index': 0, 'result': 'a'},
                         {'index': 1, 'result': 'b'},
                         {'index': 2, 'result': 'c'},
                         {'index': 3, 'result': 'd'},
                         {'index': 4, 'result': 'e'})


async def fake_task(data, second):
  """Fake task for testing."""
  await asyncio.sleep(second)
  return data


def fake_task_sync(data):
  """Fake task for testing."""
  return data


class AsyncUtilsTest(unittest.TestCase):

  def test_run_asynchronized_function_returns_expected_output(self):
    param_gen = ({'data': elem, 'second': 5} for elem in _BATCH_DATA)
    async_utils._WORKER_TIMEOUT_SECONDS = 10

    result, idx_result = async_utils.run_asynchronized_function(
        fake_task, param_gen)

    self.assertCountEqual(_BATCH_DATA, result, msg=str(result))
    self.assertCountEqual(
        _INDEXED_BATCH_RESULT, idx_result, msg=str(idx_result))

  def test_run_asynchronized_function_with_large_batch(self):
    batch_data = _BATCH_DATA * 10
    param_gen = ({'data': elem, 'second': 3} for elem in batch_data)
    async_utils._WORKER_TIMEOUT_SECONDS = 10

    result, idx_result = async_utils.run_asynchronized_function(
        fake_task, param_gen)

    self.assertCountEqual(batch_data, result, msg=str(result))
    for idx_result_item in idx_result:
      self.assertEqual(idx_result_item.get('result'),
                       batch_data[idx_result_item.get('index')])

  def test_run_asynchronized_function_exceeds_coroutine_time_limit(self):
    param_gen = ({'data': elem, 'second': 6} for elem in _BATCH_DATA)
    async_utils._WORKER_TIMEOUT_SECONDS = 5

    result, idx_result = async_utils.run_asynchronized_function(
        fake_task, param_gen)

    self.assertListEqual([], result)
    for idx_result_item in idx_result:
      self.assertIsNone(idx_result_item.get('result'))

  def test_run_synchronized_function_return_expected_output(self):
    param_gen = ({'data': elem} for elem in _BATCH_DATA)
    result = async_utils.run_synchronized_function(fake_task_sync, param_gen)

    self.assertCountEqual(_BATCH_DATA, result, msg=str(result))

if __name__ == '__main__':
  unittest.main()
