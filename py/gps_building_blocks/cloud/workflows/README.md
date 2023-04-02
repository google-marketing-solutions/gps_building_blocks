# Function Flow
A low cost, lightweight workflow orchestration framework based on Cloud Functions.

## Background
In many data and machine learning projects, we need to have an infrastructure that can manage our “workflows” or “data pipelines”. For example, consider a workflow like this:

```ingest data from GCS -> generate features with user data -> call AutoML to get predictions -> send results to Google Ads```

The workflows are usually in the form of a DAG, where each task can depend on a few other tasks. A task can be run only if all of its dependencies are successful.

One option is to use [Cloud Composer](https://cloud.google.com/composer) to orchestrate the tasks, which can manage task dependencies automatically. Unfortunately Cloud Composer needs an always-on cluster(>= 3 compute engines) to run and costs a few hundred USD/month (even if not running anything) which is not acceptable by developers who only run the workflow a few times per month.

This solution builds workflows on top of Cloud Functions, offers similar task dependency management ability to Cloud Composer, and is much cheaper and more lightweight.

## Example
Here is a simple example of using function flow.

This example job contains eight tasks: from `task_1` to `task_6` are locally
defined tasks (some using remote services as BigQuery), and `taskr1` and
`taskr2` are remote tasks, that is, tasks invoking cloud functions using pubsub
messages.

The dependency tree is:

```
task_1 --> task_2 --> task_4 -----> task_5 --> task_6
       \-> task_3 ------------/ \-> taskr1 -/
                                \-> taskr2 -/
```

The eight tasks will be scheduled according to the dependencies defined in the @task decorator.

On the `samples` directory, you will find the file `main.py` that contains the workflow definition.

## Deployment
To run the example:

1. Enable Firestore by visiting [this page](https://console.cloud.google.com/firestore). Select 'Native' mode when asked.
1. Create a BigQuery dataset called `test_dataset`.
1. cd into the `samples` folder.
1. Create a file named `requirements.txt` and add the following dependencies:

  ```
  gps-building-blocks
  pyOpenSSL
  ```

1. Deploy the cloud functions by running the following commands:

  ```
  gcloud functions deploy start --runtime python37 --trigger-http
  gcloud functions deploy scheduler --runtime python37 --trigger-topic SCHEDULE
  gcloud functions deploy external_event_listener --runtime python37 --trigger-topic SCHEDULE_EXTERNAL_EVENTS
  gcloud functions deploy sample_remote_function --runtime python37 --trigger-topic SAMPLE_REMOTE_TRIGGER
  ```

1. Create a log router to send BigQuery job complete logs into your PubSub
   topic for external messages (used by `task3`).

  ```
  PROJECT_ID=your_gcp_project_id

  gcloud logging sinks create bq_complete_sink \
      pubsub.googleapis.com/projects/$PROJECT_ID/topics/SCHEDULE_EXTERNAL_EVENTS \
       --log-filter='resource.type="bigquery_resource" \
       AND protoPayload.methodName="jobservice.jobcompleted"'

  sink_service_account=$(gcloud logging sinks describe bq_complete_sink|grep writerIdentity| sed 's/writerIdentity: //')

  gcloud pubsub topics add-iam-policy-binding SCHEDULE_EXTERNAL_EVENTS --member $sink_service_account --role roles/pubsub.publisher
  ```

The workflow can then be started by calling the `start` Cloud Function using the
HTTP trigger (Cloud Functions supports many ways of invocation including HTTP,
PubSub and others. See https://cloud.google.com/functions/docs/calling for
details).
