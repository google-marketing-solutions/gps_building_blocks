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

"""Future: the return type for async tasks.
"""

import time
from typing import Any, Mapping, Optional

from googleapiclient import discovery

import google.auth


class Result:
  """Wrapper for results of async tasks."""

  def __init__(self, trigger_id: str, is_success: bool,
               result: Optional[Any] = None, error: Optional[Any] = None):
    """Initializes the Result object.

    Args:
      trigger_id: The id associated with the async task. Needs to be unique
        across whole workflow.
      is_success: Is the task successfully finished.
      result: The result of the task.
      error: The error, typically a string message.
    """
    self.trigger_id = trigger_id
    self.is_success = is_success
    self.result = result
    self.error = error


class Future:
  """Return type for async tasks."""
  all_futures = []

  def __init_subclass__(cls, **kwargs):
    """Adds future subclass to the list of all available future classes.

    Args:
      **kwargs: other args
    """
    super().__init_subclass__(**kwargs)
    cls.all_futures.append(cls)

  def __init__(self, trigger_id: str):
    """Initializes the Future object.

    Args:
      trigger_id: The trigger id to be associated with this async task. This id
                  is used to trigger the corresponding function flow task to be
                  marked as finished. For example, in a BigQuery job, the job id
                  can be used as a trigger id.
    """
    self.trigger_id = trigger_id

  @classmethod
  def handle_message(cls, message: Mapping[str, Any]) -> Optional[Result]:
    """Handles the external message(event).

      This method needs to be overwritten by subclasses.

    Args:
      message: The message dict to be handled.
    Returns:
      A Result object, if the message can be parsed and handled, or None if the
        message is ignored.
    """
    raise NotImplementedError('Please implement class method handle_message!')


class BigQueryFuture(Future):
  """Return type for async big query task."""

  @classmethod
  def handle_message(cls, message: Mapping[str, Any]) -> Optional[Result]:
    """Handles bigquery task finish messages.

      If the message is a bigquery message, then parse it and return its status,
        otherwise just return None.

    Args:
      message: The message JSON dictionary.

    Returns:
      Parsed task result from the message or None.
    """
    if _get_value(message, 'resource.type') == 'bigquery_resource':
      bq_job_id = _get_value(
          message,
          'protoPayload.serviceData.jobCompletedEvent.job.jobName.jobId')
      location = _get_value(
          message,
          'protoPayload.serviceData.jobCompletedEvent.job.jobName.location')
      code = _get_value(message, 'protoPayload.status.code')

      if code:
        # The current behavior of BQ job status logs is empty status dict when
        # no errors (in this case code will be None), and all error codes are
        # non-zero.
        error = _get_value(message, 'protoPayload.status.message')
        return Result(trigger_id=bq_job_id, is_success=False, error=error)
      else:
        result = {'job_id': bq_job_id, 'location': location}
        return Result(result=result, trigger_id=bq_job_id, is_success=True)
    else:
      return None


class DataFlowFuture(Future):
  r"""Return type for async DataFlow task.

    To use this future, you need to set up a log router that routes DataFlow job
      complete logs into your PubSub topic for external messages. for example:

    ```
    gcloud logging sinks create dataflow_complete_sink
    pubsub.googleapis.com/projects/$PROJECT_ID/topics/$TOPIC_EXTERNAL \
      --log-filter='resource.type="dataflow_step" AND textPayload="Worker pool
      stopped."'

    sink_service_account=$(gcloud logging sinks describe dataflow_complete_sink
    |grep writerIdentity| sed 's/writerIdentity: //')

    gcloud pubsub topics add-iam-policy-binding $TOPIC_EXTERNAL \
      --member $sink_service_account --role roles/pubsub.publisher
    ```

    Upon calling, future class extracts the DataFlow job id from the
    "Worker pool stopped." logs and checks the job status using the Google Cloud
    APIs.
  """

  STATUS_CHECK_RETRY_TIMES = 10
  STATUS_CHECK_SLEEP_SECS = 10

  @classmethod
  def handle_message(cls, message: Mapping[str, Any]) -> Optional[Result]:
    """Handles DataFlow task finish messages.

      If the message is a DataFlow message, then parse it and return its status,
        otherwise just return None.

    Args:
      message: The message JSON dictionary.

    Returns:
      Parsed task result from the message or None.
    """
    if _get_value(message, 'resource.type') == 'dataflow_step':
      labels = _get_value(message, 'resource.labels')
      job_id = labels['job_id']
      region = labels['region']
      job_name = labels['job_name']

      _, project = google.auth.default()

      dataflow = discovery.build('dataflow', 'v1b3')
      request = dataflow.projects().locations().jobs().get(
          jobId=job_id,
          location=region,
          projectId=project)

      retry = cls.STATUS_CHECK_RETRY_TIMES
      while retry > 0:
        retry -= 1
        response = request.execute()
        if response['currentState'] == 'JOB_STATE_DONE':
          return Result(trigger_id=job_id, is_success=True)
        elif response['currentState'] == 'JOB_STATE_RUNNING':
          time.sleep(cls.STATUS_CHECK_SLEEP_SECS)
        else:
          error = {
              'job_id': job_id,
              'job_name': job_name,
              'state': response['currentState']
          }
          return Result(trigger_id=job_id, is_success=False, error=error)

      # Returns timeout error if running out of retries
      error = {
          'job_id': job_id,
          'job_name': job_name,
          'state': 'TIMEOUT CHECKING'
      }
      return Result(trigger_id=job_id, is_success=False, error=error)
    else:
      return None


def _get_value(obj: Mapping[str, Any], keypath: str):
  """Gets a value from a dictionary using dot-separated keys.

  Args:
    obj: A dictionary, which can be multi-level.
    keypath: Keys separated by dot.

  Returns:
    Value from the dictionary using multiple keys in order, for example
      `_get_value(d, 'a.b.c')` is equivalent to `d['a']['b']['c']`. If the key
      does not exist at any level, return None.
  """
  try:
    for key in keypath.split('.'):
      obj = obj[key]
  except KeyError:
    return None
  return obj
