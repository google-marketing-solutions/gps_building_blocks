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
import datetime
import enum
from typing import Any, Callable, List, Optional
import uuid

import google.auth
from google.cloud import firestore


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

  # db FIELDS
  FIELD_NAME = 'name'
  FIELD_STATUS = 'status'
  FIELD_TASKS = 'tasks'

  def __init__(self,
               name: str = None,
               db: firestore.Client = None,
               project: str = None):
    """Initializes the job instance.

    Args:
      name: Job name. Will also be the prefix for the id of job instance.
      db: The firestore client which stores/loads job status information to/from
        database.
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

    self.tasks = []

  def _load(self, job_id: str):
    """Loads status of job with `job_id` from database.

    Args:
      job_id: The id of the job instance.
    """
    job_ref = self._get_job_ref()
    job = job_ref.get().to_dict()
    self.name = job[self.FIELD_NAME]
    self.id = job_id

    # Loads current task status from db
    tasks_ref = self.db.collection(self.JOB_STATUS_COLLECTION, job_id,
                                   self.FIELD_TASKS)
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
          # Get the task with a matching id, there should be only one such task.
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

  def _get_job_ref(self) -> 'firestore.DocumentReference':
    """Gets job reference from database."""
    return self.db.collection(self.JOB_STATUS_COLLECTION).document(self.id)

  def task(self,
           task_id: str,
           deps: List[str] = None) -> Callable[..., Any]:
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

  def start(self, argv: Optional[List[str]] = None):
    """Starts a job.

    Args:
      argv: The start arguments.

      Creates an entry in the `JOB_STATUS_COLLECTION` storing the job and task
        status, set job status to RUNNING, and all tasks in the job to READY so
        that they can be scheduled.
    """
    # save to db
    datestamp = datetime.datetime.now().strftime('%Y-%m-%d-%H%M')
    rand_id = uuid.uuid4().hex[:4]
    self.id = f'{self.name}-{datestamp}-{rand_id}'

    job_ref = self._get_job_ref()
    job_ref.set({
        'name': self.name,
        'start_time': datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S'),
        'status': JobStatus.RUNNING,
        'argv': argv or []
    })

    tasks_ref = self.db.collection(self.JOB_STATUS_COLLECTION, self.id,
                                   self.FIELD_TASKS)
    for task in self.tasks:
      task_ref = tasks_ref.document(task.id)
      task_ref.set({
          'id': task.id,
          'deps': task.deps,
          'status': TaskStatus.READY,
      })

  def get_arguments(self):
    """Get start arguments of this job.

    Returns:
      argv paremeter of start()
    """
    job_ref = self._get_job_ref()
    job = job_ref.get().to_dict()
    return job['argv']
