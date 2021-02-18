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

"""Asynchronization utility for concurrent programming.

Usage Example:
  async def function(second):
    await asyncio.sleep(second)
    return second

  params_generator = ({'second': i} for i in range(5))

  results = async_utils.run_asynchronized_function(function, params_generator)
"""

import asyncio
import concurrent.futures
import functools
import logging
from typing import Any, Callable, Dict, Generator, List, Text, Tuple, Union


_MAX_WORKERS = 16
_WORKER_TIMEOUT_SECONDS = 10


async def _worker(
    worker_id: Text,
    queue: 'asyncio.Queue[Any]',
    async_function: Callable[..., Any],
    batch_results: List[Any],
    indexed_batch_results: List[Any]) -> None:
  """Performs a single async task.

  With worker, the concurrency of the program is controlled by specifying
  the number of workers. The result of the task will be appended to a common
  variable by thread-safe operations.

  Args:
    worker_id: The id of the worker; mainly for debug purpose.
    queue: Async queue to store awaiting task data.
    async_function: The async function to call during the task execution.
    batch_results: List of function results.
    indexed_batch_results: List of function results with index.
  """
  while True:
    result = None
    indexed_params = None
    indexed_params = await queue.get()
    index = indexed_params.get('index')
    params = indexed_params.get('params')
    try:
      result = await asyncio.wait_for(
          async_function(**params),
          timeout=_WORKER_TIMEOUT_SECONDS)
    except asyncio.TimeoutError as error:
      logging.exception(
          'Async task in worker %d exceeds the time limit. %s',
          worker_id,
          error)
    finally:
      if result:
        batch_results.append(result)
        indexed_batch_results.append({'index': index, 'result': result})
      else:
        indexed_batch_results.append({'index': index})
      queue.task_done()


async def _schedule_batch_tasks(
    async_function: Callable[..., Any],
    params_list: Union[
        Generator[Dict[Text, Any], None, None], List[Dict[Text, Any]]],
    batch_results: List[Any],
    indexed_batch_results: List[Any]) -> None:
  """Schedules batched tasks in worker pool.

  Initiate workers as coroutine and put them into event loop.

  Args:
    async_function: The async function to call during the task.
    params_list: List or Generator of the parameters to the function.
    batch_results: List of function results.
    indexed_batch_results: List of function results with index.
  """
  queue = asyncio.Queue()
  loop = asyncio.get_event_loop()

  for params in params_list:
    queue.put_nowait(params)

  tasks = []
  for i in range(_MAX_WORKERS):
    task = loop.create_task(
        _worker('worker_{}'.format(i),
                queue,
                async_function,
                batch_results,
                indexed_batch_results))
    tasks.append(task)

  await queue.join()

  # Cancel all the workers after the task queue is fully consumed.
  for task in tasks:
    task.cancel()

  # Wait until all worker tasks are cancelled.
  await asyncio.gather(*tasks, return_exceptions=True)


def run_asynchronized_function(
    async_function: Callable[..., Any],
    params_list: Union[
        Generator[Dict[Text, Any], None, None], List[Dict[Text, Any]]]
    ) -> Tuple[List[Any], List[Any]]:
  """Executes an asynchronized function with given list of parameters.

  When user wants to call a function repetitively, it could be time consuming to
  run it one after one. The asynchronization utility helps user concurrently
  call a function for multiple times. To enable concurrency, the input function
  should be an asynchronize one and the params generator should generate a list
  of params for each call of the function.

  For more information about asynchronize function in Python3, please refer to
  https://docs.python.org/3/library/asyncio.html

  Args:
    async_function: The function to call during the task.
    params_list: Generator or list of the parameters to the function.

  Returns:
    batch_results: List of task results of which the order is not guaranteed to
    be the same as given parameters.
    indexed_batch_results: List of indexed task results, containing indexes
    indicating the corresponding index in given parameters.

  Raises:
    TypeError: Raised if input function is not callable.
  """
  if not callable(async_function):
    raise TypeError(
        'Input parameter is not a Callable type. Please provide a function.')

  batch_results = []
  indexed_batch_results = []
  indexed_params_list = [{'index': idx, 'params': params}
                         for idx, params in enumerate(params_list)]

  loop = asyncio.get_event_loop()
  loop.run_until_complete(
      _schedule_batch_tasks(async_function,
                            indexed_params_list,
                            batch_results,
                            indexed_batch_results))
  return batch_results, indexed_batch_results


async def _sync_to_async(
    sync_function: Callable[..., Any],
    params_list: Union[
        Generator[Dict[Text, Any], None, None], List[Dict[Text, Any]]]
    ) -> Tuple[Any]:
  """Executes a non asynchronous function asynchronously.

  Args:
    sync_function: The function to call during the task.
    params_list: Generator or list of the parameters to the function.

  Returns:
    batch_results: List of task results.
  """
  executor = concurrent.futures.ThreadPoolExecutor(max_workers=_MAX_WORKERS)
  loop = asyncio.get_event_loop()
  futures = []
  for param in params_list:
    future = loop.run_in_executor(
        executor, functools.partial(sync_function, **param))
    futures.append(future)
  return await asyncio.gather(*futures, return_exceptions=True)  # pytype: disable=bad-return-type


def run_synchronized_function(
    sync_function: Callable[..., Any],
    params_list: Union[
        Generator[Dict[Text, Any], None, None], List[Dict[Text, Any]]]
    ) -> Tuple[Any]:
  """Executes a non asynchronous function with given list of parameters.

  This is a function to execute non asynchronous function in concurrent way.
  Users can easily leverage concurrent programming without wrapping their
  function with asynchronization by calling this function. The results are in
  the same order as the given parameters list.

  Args:
    sync_function: The function to call during the task.
    params_list: Generator or list of the parameters to the function.

  Returns:
    batch_results: List of task results in the same order as given parameters.

  Raises:
    TypeError: Raised if input function is not callable.
  """
  if not callable(sync_function):
    raise TypeError(
        'Input parameter is not a Callable type. Please provide a function.')
  loop = asyncio.get_event_loop()
  return loop.run_until_complete(_sync_to_async(sync_function, params_list))
