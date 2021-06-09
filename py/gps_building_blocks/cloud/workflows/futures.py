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

import datetime
import json
import time
from typing import Any, Mapping, Optional
import urllib

from googleapiclient import discovery

import google.auth
from google.cloud import firestore
from google.cloud import pubsub_v1
from google.cloud import storage


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
  r"""Return type for async BigQuery task.

    To use this future, you need to set up a log router that routes BigQuery job
      complete logs into your PubSub topic for external messages. for example:

    ```
    gcloud logging sinks create bq_complete_sink \
    pubsub.googleapis.com/projects/$PROJECT_ID/topics/$TOPIC_EXTERNAL \
     --log-filter='resource.type="bigquery_resource" \
     AND protoPayload.methodName="jobservice.jobcompleted"'

    sink_service_account=$(gcloud logging sinks describe bq_complete_sink
    |grep writerIdentity| sed 's/writerIdentity: //')

    gcloud pubsub topics add-iam-policy-binding $TOPIC_EXTERNAL \
      --member $sink_service_account --role roles/pubsub.publisher
    ```

    Upon calling, future class extracts the BigQuery job status from the log
    message directly: if a "status.code" is present the job failed, otherwise
    it completed successfully.
  """

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


class GCSFuture(Future):
  """A future type fullfilled by creation of a GCS path prefix."""

  # The internal event type to match against the pubsub message
  GCS_EVENT_TYPE = 'gcs_path_create'

  def __init__(self, path_prefix: str):
    """Initializes the object.

    Args:
      path_prefix: The path prefix to watch.
    """
    trigger_id = urllib.parse.quote(path_prefix, safe='')
    super().__init__(trigger_id)
    GCSPoller.register_path_prefix(path_prefix)

  @classmethod
  def handle_message(cls, message: Mapping[str, Any]) -> Optional[Result]:
    """Handles GCS path creation messages.

    Args:
      message: The message JSON dictionary.

    Returns:
      Parsed task result from the message or None.
    """
    if _get_value(message, 'function_flow_event_type') == cls.GCS_EVENT_TYPE:
      path_prefix = _get_value(message, 'path_prefix')
      trigger_id = urllib.parse.quote(path_prefix, safe='')
      return Result(result=path_prefix, trigger_id=trigger_id, is_success=True)
    else:
      return None


class GCSPoller:
  """Polls GCS for existence of path prefixes."""

  # Collection storing GCS path prefixes to watch.
  GCS_WATCH_COLLECTION = 'GCSWatches'

  def __init__(self,
               event_topic: str,
               db: Optional[firestore.Client] = None,
               pubsub: Optional[pubsub_v1.PublisherClient] = None,
               project: Optional[str] = None):
    """Initializes the object.

    Args:
      event_topic: The PubSub topic for external event.
      db: The Firestore database client.
      pubsub: The Cloud PubSub client.
      project: The GCP project ID.
    """
    self.db = db or firestore.Client()
    self.pubsub = pubsub or pubsub_v1.PublisherClient()
    if not project:
      _, project = google.auth.default()
    self.topic_path = self.pubsub.topic_path(project, event_topic)

  def poll(self):
    """Polls GCS for existence of stored path prefixes.

    When a watched path exists, sends out a PubSub event which in turn triggers
      the corresponding GCSFuture to be fullfilled.
    """
    gcs_watches = self.db.collection(self.GCS_WATCH_COLLECTION)

    for watch in gcs_watches.stream():
      path_prefix = urllib.parse.unquote(watch.id)

      parse_result = urllib.parse.urlparse(path_prefix)
      bucket_name = parse_result.netloc
      prefix = parse_result.path[1:]

      storage_client = storage.Client()
      blobs = storage_client.list_blobs(
          bucket_name, prefix=prefix, delimiter=None
      )

      path_exist = False
      for _ in blobs:
        path_exist = True
        break

      if path_exist:
        # Constructs an event and sends it to pubsub
        message = {
            'function_flow_event_type': GCSFuture.GCS_EVENT_TYPE,
            'path_prefix': path_prefix
        }
        data = json.dumps(message).encode('utf-8')
        self.pubsub.publish(self.topic_path, data=data)
        self.deregister_path_prefix(path_prefix, db=self.db)

  @classmethod
  def register_path_prefix(cls, path_prefix: str, db=None):
    """Regsiters a GCS path prefix to be watched.

    Args:
      path_prefix: The GCS path prefix.
      db: The Firestore database client.
    """
    db = db or firestore.Client()
    gcs_watches = db.collection(cls.GCS_WATCH_COLLECTION)
    # Firestore can not have '/' in document ID, so it's neccessary to quote it.
    path_prefix_escaped = urllib.parse.quote(path_prefix, safe='')
    gcs_watches.document(path_prefix_escaped).set({
        'created': datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
    })

  @classmethod
  def deregister_path_prefix(cls, path_prefix: str, db=None):
    """Deregsiters a GCS path prefix so that is no longer watched.

    Args:
      path_prefix: The GCS path prefix.
      db: The Firestore database client.
    """
    db = db or firestore.Client()
    gcs_watches = db.collection(cls.GCS_WATCH_COLLECTION)
    path_prefix_escaped = urllib.parse.quote(path_prefix, safe='')
    gcs_watches.document(path_prefix_escaped).delete()


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
