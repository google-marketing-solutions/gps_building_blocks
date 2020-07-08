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

from typing import Any, Callable, List
import google.auth


class TaskStatus:
  """Represents the status of a single task.
  """
  # ready to be scheduled
  READY = 'READY'
  # running
  RUNNING = 'RUNNING'
  # failed by raising errors
  FAILED = 'FAILED'
  # finished
  FINISHED = 'FINISHED'


class Task:
  """Class representing a task.

    A task is a work unit and will be executed during a single invocation of a
      cloud function.
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

    Users first create a job instance, then defines tasks like this:
    ```python
      job = Job(name=..., max_parallel_tasks=...)

      @job.task(task_id='task1', deps=[...])
      def task1(job, task):
        # task logic
        ...
    ```
  """

  def __init__(self,
               name: str = None,
               project: str = None):
    """Constructor.

    Args:
      name: Job name. Will also be the prefix for the id of job instance.
      project: The cloud project id. Will be determined from current envrioment
        automatically(for example the project of the cloud function) if not
        specified.
    """
    self.name = name
    if not project:
      _, project = google.auth.default()
    self.project = project
    self.tasks = []

  def _get_runnable_tasks(self) -> List[Task]:
    """Get all runnable tasks in the job.

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
      task = Task(task_id=task_id, deps=deps, func=task_func)
      self.tasks.append(task)

    return wrapper
