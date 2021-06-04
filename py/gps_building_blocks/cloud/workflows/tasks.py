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

"""Task scheduler for Function Flow.
"""
import base64
import datetime
import enum
import json
import logging
import random
import traceback
from typing import Any, Callable, Dict, List, Optional
import uuid

from google.api_core import exceptions
import google.auth
from google.cloud import firestore
from google.cloud import pubsub_v1

from gps_building_blocks.cloud.workflows import futures


_ALREADY_EXISTS_CODE = 6


class _AutoName(enum.Enum):
  """Automatically generate enum values as strings."""

  def _generate_next_value_(name, start, count, last_values):
    """Automatically generate enum values as strings.

      Ref: https://docs.python.org/3/library/enum.html#using-automatic-values

    Args:
      start: start
      count: count
      last_values: last generated values

    Returns:
      The same name as enum variable name
    """
    return name


class TaskStatus(str, _AutoName):
  """Represents the status of a single task.
  """
  # ready to be scheduled
  READY = enum.auto()
  # running
  RUNNING = enum.auto()
  # failed by raising errors
  FAILED = enum.auto()
  # finished
  FINISHED = enum.auto()


class JobStatus(str, _AutoName):
  """Represents the status of a job(aka workflow)."""
  # running
  RUNNING = enum.auto()
  # A job fails if any of the tasks in the job fails
  FAILED = enum.auto()
  # finished
  FINISHED = enum.auto()


class Task:
  """A work unit that gets executed by a cloud function.

    Attributes:
      id: The task id. Unique within the same job.
      deps: The list of task ids that this task depends on.
      func: The function to be executed for this task.
      status: The task status. See class doc of `TaskStatus` for details.
  """

  def __init__(self,
               task_id: str,
               deps: List[str],
               func: Callable[['Task', 'Job'], Any]):
    """Constructor.

    Args:
      task_id: The task id.
      deps: The task ids list of the dependencies(tasks that should be executed
        before this task).
      func: The function to be called during execution of this task. This
        function is defined using `@job.task` decorator, and will be called as
        `func(task=task_instance, job=job_instance)`.
    """
    self.id = task_id
    self.deps = deps
    self.func = func
    self.status = TaskStatus.READY


class Job:
  """A job (aka workflow) is a DAG of tasks.

    Tasks belonging to the same job are grouped together to manage their
      execution order, statuses and results.

    Users first create a job instance, then define tasks like this:
    ```python
      job = Job(name=..., max_parallel_tasks=...)

      @job.task(task_id='task1', deps=[...])
      def task1(job, task):
        # task logic
        ...
    ```
  """

  # The database collection to store job and task status
  JOB_STATUS_COLLECTION = 'JobStatus'

  # The database collection to store external event triggers
  EVENT_TRIGGERS_COLLECTION = 'EventTriggers'

  # db FIELDS
  FIELD_NAME = 'name'
  FIELD_STATUS = 'status'
  FIELD_TASKS = 'tasks'

  def __init__(self,
               name: str,
               db: Optional[firestore.Client] = None,
               pubsub: Optional[pubsub_v1.PublisherClient] = None,
               schedule_topic: str = 'SCHEDULE',
               external_event_topic: str = 'SCHEDULE_EXTERNAL_EVENTS',
               max_parallel_tasks: int = 5,
               project: Optional[str] = None):
    """Initializes the job instance.

    Args:
      name: Job name. Will also be the prefix for the id of job instance.
      db: The firestore client which stores/loads job status information to/from
        database.
      pubsub: The pubsub client.
      schedule_topic: The topic which triggers the scheduler function.
      external_event_topic: The topic for external events, e.g. completion log
        for BigQuery jobs, etc.
      max_parallel_tasks: Maximum tasks running in parallel.
      project: The cloud project id. Will be determined from current envrioment
        automatically(for example the project of the cloud function) if not
        specified.
    """
    self.name = name
    if not project:
      _, project = google.auth.default()
    self.project = project
    self.id = None

    self.db = db or firestore.Client(project=project)
    self.max_parallel_tasks = max_parallel_tasks
    self.pubsub = pubsub or pubsub_v1.PublisherClient()
    self.external_event_topic = external_event_topic
    self.topic_path = self.pubsub.topic_path(project, schedule_topic)
    # create the topic for scheduling messages if not exist
    try:
      self.pubsub.create_topic(self.topic_path)
    except exceptions.GoogleAPICallError as e:
      if e.grpc_status_code.value[0] != _ALREADY_EXISTS_CODE:
        raise

    self.tasks = []

  def _load(self, job_id: str):
    """Loads status of job with `job_id` from database.

    Args:
      job_id: The id of the job instance.
    """
    self.id = job_id
    job_ref = self._get_job_ref()
    job = job_ref.get().to_dict()
    self.name = job[self.FIELD_NAME]

    # Loads current task status from db
    tasks_ref = self._get_tasks_ref()
    tasks = {task.id: task.to_dict() for task in tasks_ref.stream()}
    for task in self.tasks:
      task.status = tasks[task.id][self.FIELD_STATUS]

  def _get_runnable_tasks(self) -> List[Task]:
    """Gets all runnable tasks in the job.

      A task is runnable if:
        1. It's in the READY state.
        2. All of its dependent tasks are in the FINISHED state.

    Returns:
      The list of runnable tasks.
    """
    runnable_tasks = []

    for task in self.tasks:
      if task.status != TaskStatus.READY:
        # Task cannot be run if it is not in the READY state
        continue

      if not task.deps:
        # Task can run if it has no dependencies
        runnable_tasks.append(task)
      else:
        can_run = True
        for dep_task_id in task.deps:
          # Gets the (unique)task with a matching id
          dep_task = None
          for t in self.tasks:
            if t.id == dep_task_id:
              dep_task = t
              break
          # If any dependent task is not finished, then the task cannot be run
          if dep_task is not None and dep_task.status != TaskStatus.FINISHED:
            can_run = False
            break
        if can_run:
          runnable_tasks.append(task)

    return runnable_tasks

  def _get_job_ref(self) -> firestore.DocumentReference:
    """Gets job reference from database."""
    return self.db.collection(self.JOB_STATUS_COLLECTION).document(self.id)

  def _get_tasks_ref(self) -> firestore.CollectionReference:
    """Gets tasks reference of current job from database."""
    return self.db.collection(self.JOB_STATUS_COLLECTION,
                              self.id, self.FIELD_TASKS)

  def _get_trigger_ref(self, trigger_id: str) -> firestore.DocumentReference:
    """Gets a trigger reference from database.

    Args:
      trigger_id: The trigger id.
    Returns:
      Trigger reference.
    """
    return self.db.collection(
        self.EVENT_TRIGGERS_COLLECTION).document(trigger_id)

  def _transition_task_state(self,
                             task_ref: firestore.DocumentReference,
                             from_state: TaskStatus,
                             to_state: str,
                             updates: Optional[Dict[Any, Any]] = None) -> bool:
    """Transitions task states.

      If the task is in `from_state`, this method transitions it to
        `to_state`, writes additional updates to the task document and returns
        True. Otherwise this just returns False without doing anything else.

    Args:
      task_ref: ref to task object in the database.
      from_state: The state before transition.
      to_state: The state after transition.
      updates: Dictionary containing other updates to be written.
    Returns:
      True if the transition happened, False otherwise.
    """
    transaction = self.db.transaction()

    @firestore.transactional
    def transition_task(transaction):
      """The transaction to transition the task state."""
      snapshot = task_ref.get(transaction=transaction)
      if snapshot.get('status') == from_state:
        transaction.update(task_ref, dict(status=to_state, **(updates or {})))
        return True
      return False

    return transition_task(transaction)

  def _start_task(
      self,
      task_ref: firestore.DocumentReference) -> bool:
    """Marks a task as running in the database, if it's not started yet.

    Args:
      task_ref: ref to task object in the database.
    Returns:
      True if the task changes from READY to RUNNING state, otherwise False.
    """
    return self._transition_task_state(task_ref,
                                       from_state=TaskStatus.READY,
                                       to_state=TaskStatus.RUNNING)

  def _finish_task(
      self,
      task_ref: firestore.DocumentReference,
      result: Optional[Any] = None) -> bool:
    """Marks a running task as finished in the database.

    Args:
      task_ref: The ref to task object in the database.
      result: The result of the task, to be written in the task entry.
    Returns:
      True if the task changes from RUNNING to FINISHED state, otherwise False.
    """
    for task in self.tasks:
      if task.id == task_ref.id:
        task.status = TaskStatus.FINISHED

    return self._transition_task_state(
        task_ref,
        from_state=TaskStatus.RUNNING,
        to_state=TaskStatus.FINISHED,
        updates={'result': result} if result else None)

  def _fail_task(
      self,
      task_ref: firestore.DocumentReference,
      error: Any) -> bool:
    """Marks a running task as failed in the database.

    Args:
      task_ref: The ref to task object in the database.
      error: The error message of the task, to be written in the task entry.
    Returns:
      True if the task changes from RUNNING to FAILED state, otherwise False.
    """
    # updates job status to FAILED
    job_ref = self._get_job_ref()
    job_ref.update({'status': JobStatus.FAILED, 'error': error})

    return self._transition_task_state(task_ref,
                                       from_state=TaskStatus.RUNNING,
                                       to_state=TaskStatus.FAILED,
                                       updates={'error': error})

  def _publish_schedule_message(self,
                                count: Optional[int] = None):
    """Publish scheduling messages to initiate following tasks.

    Args:
      count: The number of messages to publish. A count of more than 1 allows
             multiple tasks to be scheduled. A none value means sending
             according to max parallel task settings.
    """
    if count is None:
      num_running_tasks = len(
          [t for t in self.tasks if t.status == TaskStatus.RUNNING])
      count = self.max_parallel_tasks - num_running_tasks

    if count <= 0:
      return

    message = {'id': self.id}
    data = json.dumps(message).encode('utf-8')
    for _ in range(count):
      self.pubsub.publish(self.topic_path, data=data)

  def _schedule(self):
    """Schedules one task from the runnable tasks to run.
    """

    all_finished = True
    for task in self.tasks:
      logging.info('job: %s task: %s status: %s', self.id, task.id, task.status)
      if task.status != TaskStatus.FINISHED:
        all_finished = False
        break
    if all_finished:
      job_ref = self._get_job_ref()
      job_ref.update({
          'status': JobStatus.FINISHED,
          'finish_time': datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
      })
      return

    tasks = self._get_runnable_tasks()
    if not tasks:
      logging.info('No tasks ready to run, return')
      return

    task = random.choice(tasks)
    tasks_ref = self._get_tasks_ref()
    task_ref = tasks_ref.document(task.id)
    if self._start_task(task_ref):
      try:
        result = task.func(task, self)
        if isinstance(result, futures.Future):
          # async task
          self._register_async_task_trigger(task.id, result.trigger_id)
          task_ref.update({'trigger_id': result.trigger_id})
        else:
          # set task as finished
          self._finish_task(task_ref, result)
          self._publish_schedule_message()
      except:
        # Intentionally catches all exceptions during the execution of the task,
        # so that errors can be saved in the task status.
        logging.exception('Error encountered in task')
        err_msg = traceback.format_exc()
        # marks task as failed
        self._fail_task(task_ref, err_msg)
        raise

  def _register_async_task_trigger(self, task_id: str, trigger_id: str):
    """Registers a trigger for an async task.

      The trigger is used to match the async task(e.g. the BigQuery job you run
      asynchronously) with the function flow job and task.

    Args:
      task_id: The task id.
      trigger_id: The trigger id.
    """
    trigger_ref = self._get_trigger_ref(trigger_id)
    trigger_ref.set({
        'job_id': self.id,
        'task_id': task_id
    })

  def _handle_event(self, message: Dict[str, Any]):
    """Handles external events (e.g. finish event of big query jobs).

    Args:
      message: Event message.
    """
    logging.info('handle message: %s', message)

    for future_cls in futures.Future.all_futures:
      result = future_cls.handle_message(message)

      if not result:
        # The message is not matched by current future class, ignore
        continue

      trigger_id = result.trigger_id
      trigger_ref = self._get_trigger_ref(trigger_id)
      trigger = trigger_ref.get().to_dict()

      if trigger:
        job_id = trigger['job_id']
        task_id = trigger['task_id']
        self._load(job_id)
        tasks_ref = self._get_tasks_ref()
        task_ref = tasks_ref.document(task_id)

        if result.is_success:
          self._finish_task(task_ref, result=result.result)
          self._publish_schedule_message()
        else:
          self._fail_task(task_ref, error=result.error)

  def task(self,
           task_id: str,
           deps: Optional[List[str]] = None) -> Callable[..., Any]:
    """Task wrapper for creating tasks in this job.

    Args:
      task_id: The id for the task.
      deps: The ids for the dependent tasks.

    Returns:
      A wrapped task function.
    """
    def wrapper(task_func):
      task = Task(task_id=task_id, deps=deps or [], func=task_func)
      self.tasks.append(task)

    return wrapper

  def start(self, argv: Optional[Dict[str, Any]] = None):
    """Starts a job.

    Args:
      argv: The start arguments.

      Creates an entry in the `JOB_STATUS_COLLECTION` storing the job and task
        status, sets job status to RUNNING, and all tasks in the job to READY so
        that they can be scheduled.
    """
    # creates a job document and saves it to db
    datestamp = datetime.datetime.now().strftime('%Y-%m-%d-%H%M')
    rand_id = uuid.uuid4().hex[:4]
    self.id = f'{self.name}-{datestamp}-{rand_id}'

    job_ref = self._get_job_ref()
    job_ref.set({
        'name': self.name,
        'start_time': datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S'),
        'status': JobStatus.RUNNING,
        'argv': argv or {}
    })

    # creates task entries in the job document
    tasks_ref = self._get_tasks_ref()
    for task in self.tasks:
      task_ref = tasks_ref.document(task.id)
      task_ref.set({
          'id': task.id,
          'deps': task.deps,
          'status': TaskStatus.READY,
      })

    self._publish_schedule_message()

  def get_arguments(self):
    """Gets start arguments of this job.

    Returns:
      argv paremeter of start()
    """
    job_ref = self._get_job_ref()
    job = job_ref.get().to_dict()
    return job['argv']

  def get_task_result(self, task_id: str):
    """Gets task result from this job.

    Args:
      task_id: Task Id.

    Returns:
      Result returned by task and saved into database.
    """
    tasks_ref = self._get_tasks_ref()
    task = tasks_ref.document(task_id).get().to_dict()
    return task['result']

  def make_scheduler(self):
    """Creates a job scheduler function which can be called as a cloud function.

    Returns:
      The scheduler function.
    """
    def scheduler(event, unused_context):
      message = base64.b64decode(event['data']).decode('utf-8')
      args = json.loads(message)
      self._load(job_id=args['id'])
      self._schedule()
    return scheduler

  def make_external_event_listener(self):
    """Creates a function to receive external events (e.g. bq job finish).

    Returns:
      The event listener function.
    """
    def listener(event, context):
      del context  # unused context
      message = base64.b64decode(event['data']).decode('utf-8')
      try:
        message = json.loads(message)
        self._handle_event(message)
      except json.JSONDecodeError:
        # Intentionally ignore unwanted messages
        pass

    return listener

  def make_poller(self):
    """Creates a function to poll external events.

    This is useful when there is an external service we call asynchronously, and
      polls the status to check its completion status(as an example, use AutoML
      output path to decide when the prediction is finished).

    Returns:
      A function which can be used as a cloud function, triggered by Cloud
      Ccheduler.
    """
    gcs_poller = futures.GCSPoller(self.external_event_topic)

    def poll_fn(*args, **kwargs):
      del args, kwargs  # unused
      gcs_poller.poll()

    return poll_fn


def cleanup_expired_jobs(db=None, project=None, max_expire_days=30):
  """Clean up database entries for expired job statuses.

  Args:
    db: Firestore db instance.
    project: GCP project id.
    max_expire_days: Maximum expire days for a job.
  """
  if not db:
    db = firestore.Client(project=project)

  jobs_ref = db.collection(Job.JOB_STATUS_COLLECTION)
  now = datetime.datetime.now()
  expired_job_ids = []
  # Find all expired jobs
  for job_ref in jobs_ref.stream():
    job = job_ref.to_dict()
    start_time = datetime.datetime.strptime(job['start_time'],
                                            '%Y-%m-%d-%H:%M:%S')
    if (now - start_time).days >= max_expire_days:
      expired_job_ids.append(job_ref.id)

  logging.info('%d jobs to delete', len(expired_job_ids))
  # For each expired job, first delete all tasks inside the job,
  # then delete the job itself.
  for job_id in expired_job_ids:
    job_ref = jobs_ref.document(job_id)
    tasks_ref = db.collection(Job.JOB_STATUS_COLLECTION,
                              job_id, Job.FIELD_TASKS)
    for task_ref in tasks_ref.stream():
      task_ref.reference.delete()
    job_ref.delete()
    logging.info('Deleted expired job status: %s', job_id)
