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

"""Tests for google3.third_party.gps_building_blocks.py.cloud.workflows.tasks."""

import unittest
from gps_building_blocks.cloud.workflows import tasks


class TasksTest(unittest.TestCase):

  def test_can_create_job(self):
    job = tasks.Job(name='test_job', project='test_project')
    self.assertEqual(job.name, 'test_job')
    self.assertEqual(job.project, 'test_project')

  def test_can_create_tasks(self):
    job = tasks.Job(name='test_job', project='test_project')

, unused-variable
    @job.task(task_id='task1')
    def task1(job, task):
      return 'result1'

    @job.task(task_id='task2')
    def task2(job, task):
      return 'result2'
, unused-variable

    task_ids = sorted([task.id for task in job.tasks])
    self.assertListEqual(task_ids, ['task1', 'task2'])

  def test_task_dependency(self):
    job = tasks.Job(name='test_job', project='test_project')

, unused-variable
    @job.task(task_id='task1')
    def task1(job, task):
      return 'result1'

    @job.task(task_id='task2', deps=['task1'])
    def task2(job, task):
      return 'result2'

    @job.task(task_id='task3', deps=['task1'])
    def task3(job, task):
      return 'result3'

    @job.task(task_id='task4', deps=['task2', 'task3'])
    def task4(job, task):
      return 'result4'
, unused-variable

    # The task dependency is set up as follows:
    # task1 --> task2 --> task4
    #       \-> task3 -/
    # So at first, only task1 is runnable.

    runnable_tasks = [task.id for task in job._get_runnable_tasks()]
    self.assertListEqual(runnable_tasks, ['task1'])

    for task in job.tasks:
      if task.id == 'task1':
        task.status = tasks.TaskStatus.FINISHED

    # After task1 is finished, task2 and task3 should be able to run
    runnable_tasks = sorted([task.id for task in job._get_runnable_tasks()])
    self.assertListEqual(runnable_tasks, ['task2', 'task3'])

    for task in job.tasks:
      if task.id == 'task2':
        task.status = tasks.TaskStatus.FINISHED
      if task.id == 'task3':
        task.status = tasks.TaskStatus.RUNNING

    # When task2 is finished but task3 is not, task4 cannot be run yet.
    runnable_tasks = [task.id for task in job._get_runnable_tasks()]
    self.assertEqual(len(runnable_tasks), 0)

    for task in job.tasks:
      if task.id == 'task3':
        task.status = tasks.TaskStatus.FINISHED

    # Finally, after task1-3 are finished, task4 is able to run
    runnable_tasks = [task.id for task in job._get_runnable_tasks()]
    self.assertListEqual(runnable_tasks, ['task4'])


if __name__ == '__main__':
  unittest.main()
