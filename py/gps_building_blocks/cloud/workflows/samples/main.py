# python3
# coding=utf-8
# Copyright 2021 Google LLC.
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
"""Function Flow demo script."""
import base64
import json
import logging
import time

import google.auth
from google.cloud import bigquery
from google.cloud import pubsub_v1
from gps_building_blocks.cloud.workflows import futures
from gps_building_blocks.cloud.workflows import tasks

example_job = tasks.Job(
    name='test_job', schedule_topic='SCHEDULE', max_parallel_tasks=2)


@example_job.task(task_id='task_1')
def task_1(task: tasks.Task, job: tasks.Job) -> str:
  """task_1: a simple task that returns a string."""
  del task, job
  logging.info('Running task_1.')
  return 'result1'


@example_job.task(task_id='task_2', deps=['task_1'])
def task_2(task: tasks.Task, job: tasks.Job) -> str:
  """task_3: a simple task that returns a string."""
  del task, job
  logging.info('Running task_2.')
  return 'result2'


@example_job.task(task_id='task_3', deps=['task_1'])
def task_3(task: tasks.Task, job: tasks.Job) -> str:
  """task_3: a simple task that returns a string."""
  del task, job
  logging.info('Running task_3.')
  return 'result3'


@example_job.task(task_id='task_4', deps=['task_2'])
def task_4(task: tasks.Task, job: tasks.Job) -> str:
  """task_4: a simple task that returns a string."""
  del task, job
  logging.info('Running task_4.')
  return 'result4'


@example_job.task(task_id='task_5', deps=['task_3', 'task_4'])
def task_5(task: tasks.Task, job: tasks.Job) -> str:
  """task_5: a simple task that returns a string."""
  del task, job
  logging.info('Running task_5.')
  return 'result5'


@example_job.task(
    task_id='remote_task_1',
    deps=['task_3', 'task_4'],
    remote_topic='SAMPLE_REMOTE_TRIGGER',
    task_args={'region': 'APAC'})
def remote_task_1(task: tasks.Task, job: tasks.Job) -> str:
  """remote_task_1: a simple task calling a remote function."""
  del task, job
  logging.info('Running remote_task_1.')
  # The returned result will be passed to the remote function.
  return 'resultr1'


@example_job.task(
    task_id='remote_task_2',
    deps=['task_3', 'task_4'],
    remote_topic='SAMPLE_REMOTE_TRIGGER',
    task_args={'region': 'EMEA'})
def remote_task_2(task: tasks.Task, job: tasks.Job) -> str:
  """remote_task_2: a simple task calling a remote function."""
  del task, job
  logging.info('Running remote_task_2.')
  # The returned result will be passed to the remote function
  return {'strutured_data_r2': 'structured_value_r2'}


@example_job.task(
    task_id='task_6',
    deps=['task_5', 'remote_task_1', 'remote_task_2'],
    task_args={'dest_table': 'test_dataset.test_table'})
def task_6(task: tasks.Task, job: tasks.Job) -> str:
  """task06: a simple task creating a BigQuery asynchronous job."""
  del job
  logging.info('Running task_6.')
  _, project = google.auth.default()
  dst_table_id = f'{project}.{task.task_args["dest_table"]}'
  client = bigquery.Client()
  job_config = bigquery.QueryJobConfig(
      destination=dst_table_id,
      use_legacy_sql=False,
      write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE)

  sql = """
        SELECT * FROM bigquery-public-data.samples.shakespeare
        WHERE corpus = 'hamlet'
        ORDER BY word_count DESC
        LIMIT 10
  """

  # The `shakespeare` sample dataset is located in the US region. If you get an
  # error about the dataset not being available in your location,
  # try using a different dataset, or one of your own. For example, for EU
  # region you can use this query:
  #
  # sql = """
  #       SELECT rental_id, duration, end_station_name
  #       FROM `bigquery-public-data.london_bicycles.cycle_hire`
  #       ORDER BY start_date DESC
  #       LIMIT 10;
  # """

  query_job = client.query(sql, job_config=job_config)
  bq_job_id = query_job.job_id
  logging.info('Launched bq job %s.', bq_job_id)
  return futures.BigQueryFuture(bq_job_id)


# The cloud function to schedule next tasks to run.
scheduler = example_job.make_scheduler()

# The cloud function triggered by external events(e.g. finished bigquery jobs)
external_event_listener = example_job.make_external_event_listener()


def start(event, context):
  """The workflow entry point."""
  del event, context
  logging.info('Starting the workflow.')
  job_args = {
      'variable': 'my_value',
      'another_one': 'this is it',
      'a_number': 1,
      'a_dict': {
          'a': 1,
          'b': 2,
          'c': 'three'
      }
  }
  example_job.start(job_args)
  return json.dumps({'id': example_job.id})


# Sample Remote Function (also added to this file for simplicity)
def sample_remote_function(event, context):
  """This is a sample remote function that takes 30 seconds to complete.

  This functions waits 30 seconds and sends back a pubsub message before
  returning.

  Args:
    event: The dictionary with data specific to this type of event.
    context : Metadata of triggering event.
  """

  del context
  logging.info('Starting the sample remote function.')

  # Print the decoded arguments received
  message = base64.b64decode(event['data']).decode('utf-8')
  args = json.loads(message)
  logging.info('Received args: %s', args)

  time_to_sleep = 30  # seconds
  logging.info('Now going to sleep for %d seconds...', time_to_sleep)
  time.sleep(time_to_sleep)

  logging.info('Work done. Sending the signal.')

  _, project = google.auth.default()
  pubsub_publisher = pubsub_v1.PublisherClient()
  topic_path = pubsub_publisher.topic_path(project, 'SCHEDULE_EXTERNAL_EVENTS')
  generic_message = {
      'status': {
          'code': 0,  # 0 represents success, any other value represents failure
          'message': 'Everything went well.'
      },
      'resource': {
          'type': 'remote_function_resource',
          'labels': {
              'job_id': args['job_id']
          }
      }
  }
  future = pubsub_publisher.publish(topic_path,
                                    json.dumps(generic_message).encode('utf-8'))
  future.result()
  return
