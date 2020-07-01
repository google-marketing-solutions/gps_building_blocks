# Function Flow: Building workflows on Cloud Functions.

## Example
Here is a simple example of using function flow.

This example job contains four tasks from task 1 to task 4. The dependency is

```
task1 --> task2 --> task4
      \-> task3 -/
```

The four tasks will be scheduled according to the dependencies defined in the @task decorator.

```python
import json
import logging
from typing import Dict

from workflows import futures
from workflows import tasks
from google.cloud import bigquery
import google.auth

# Create this BQ table to run the example. See `Deployment` section for details.
TEST_BQ_TABLE_NAME = 'test_dataset.test_table'

example_job = tasks.Job(name='test_job',
                        schedule_topic='SCHEDULE')

@example_job.task(task_id='step1')
def task1(**unused_context: Dict[str, str]) -> str:
  """Task 1: a simple task that returns a string."""
  return 'result1'


@example_job.task(task_id='step2', deps=['step1'])
def task2(**unused_context: Dict[str, str]) -> str:
  """Task 2: a simple task that returns a string."""
  return 'result2'


@example_job.task(task_id='step3', deps=['step1'])
def task3(**unused_context: Dict[str, str]) -> str:
  """Task 3: a BigQuery asynchronous job."""
  _, project = google.auth.default()
  dst_table_id = f'{project}.{TEST_BQ_TABLE_NAME}'
  client = bigquery.Client()
  job_config = bigquery.QueryJobConfig(
      destination=dst_table_id,
      write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE)

  sql = f"""
      SELECT id, content
      FROM `{project}.{TEST_BQ_TABLE_NAME}`
  """

  query_job = client.query(sql, job_config=job_config)
  bq_job_id = query_job.job_id
  return futures.BigQueryFuture(bq_job_id)


@example_job.task(task_id='step4', deps=['step2', 'step3'])
def task4(job: tasks.Job, **unused_context: Dict[str, str]) -> str:
  """Task 4: a job that checks the result of task 2."""
  result2 = job.get_task_result('step2')
  logging.info('in task4, got task2 result: %s', result2)
  assert result2 == 'result2'
  return 'result4'

# The cloud function to schedule next tasks to run.
scheduler = example_job.make_scheduler()

# The cloud function triggered by external events(e.g. finished bigquery jobs)
external_event_listener = example_job.make_external_event_listener()


def start(unused_request: 'flask.Request') -> str:
  """The workflow entry point."""
  example_job.start()
  return json.dumps({'id': example_job.id})
```

## Deployment
To run the example, first create a table called `test_dataset.test_table` in your BigQuery, and add some fake data.
The table schema should be `(id: String, content: String)`.
To deploy the previous example, save the code as `main.py` and run the following commands:

```
gcloud functions deploy start --runtime python37 --trigger-http --source workflow
gcloud functions deploy scheduler --runtime python37 --trigger-topic SCHEDULE --source workflow
gcloud functions deploy external_event_listener --runtime python37 --trigger-topic SCHEDULE_EXTERNAL_EVENTS --source workflow
```

The workflow can then be started by calling the `start` Cloud Function. (Cloud
Functions supports many ways of invocation including HTTP, PubSub and others.
See https://cloud.google.com/functions/docs/calling for details.)
