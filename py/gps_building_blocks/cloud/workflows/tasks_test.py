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
from unittest import mock
from google.cloud import firestore

from gps_building_blocks.cloud.workflows import tasks


class TasksTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.addCleanup(mock.patch.stopall)
    self.mock_db = mock.patch.object(
        firestore, 'Client', autospec=True).start()

  def test_can_create_job(self):
    job = tasks.Job(name='test_job', project='test_project', db=self.mock_db)
    self.assertEqual(job.name, 'test_job')
    self.assertEqual(job.project, 'test_project')

  def test_can_create_tasks(self):
    job = tasks.Job(name='test_job', project='test_project', db=self.mock_db)

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
    job = tasks.Job(name='test_job', project='test_project', db=self.mock_db)

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

  def test_start_job_should_create_db_entries(self):
    job = tasks.Job(name='test_job',
                    project='test_project',
                    db=self.mock_db)

, unused-variable
    @job.task(task_id='task1')
    def task1(job, task):
      return 'result1'

    @job.task(task_id='task2', deps=['task1'])
    def task2(job, task):
      return 'result2'
, unused-variable

    job.start()
    self.mock_db.collection.assert_any_call(tasks.Job.JOB_STATUS_COLLECTION)
    self.mock_db.collection.assert_any_call(tasks.Job.JOB_STATUS_COLLECTION,
                                            mock.ANY, tasks.Job.FIELD_TASKS)

    # check job & task entries are properly set

    args_list_doc = self.mock_db.collection().document.call_args_list
    self.assertIn('test_job', args_list_doc[0][0][0])
    self.assertEqual('task1', args_list_doc[1][0][0])
    self.assertEqual('task2', args_list_doc[2][0][0])

    args_list_set = self.mock_db.collection().document().set.call_args_list
    set_job = args_list_set[0][0][0]
    self.assertEqual(set_job['name'], 'test_job')
    self.assertEqual(set_job['status'], tasks.JobStatus.RUNNING)
    set_task1 = args_list_set[1][0][0]
    self.assertEqual(set_task1['id'], 'task1')
    self.assertListEqual(set_task1['deps'], [])
    self.assertEqual(set_task1['status'], tasks.TaskStatus.READY)
    set_task2 = args_list_set[2][0][0]
    self.assertEqual(set_task2['id'], 'task2')
    self.assertListEqual(set_task2['deps'], ['task1'])
    self.assertEqual(set_task2['status'], tasks.TaskStatus.READY)

  def test_load_job(self):
    job = tasks.Job(name='test_job',
                    project='test_project',
                    db=self.mock_db)

, unused-variable
    @job.task(task_id='task1')
    def task1(job, task):
      return 'result1'

    @job.task(task_id='task2', deps=['task1'])
    def task2(job, task):
      return 'result2'
, unused-variable

    for task in job.tasks:
      if task.id == 'task1':
        self.assertEqual(task.status, tasks.TaskStatus.READY)
      if task.id == 'task2':
        self.assertEqual(task.status, tasks.TaskStatus.READY)

    self.mock_db.collection().document().get().to_dict.return_value = {
        'name': 'test_job'
    }
    task1 = mock.Mock()
    task1.id = 'task1'
    task1.to_dict.return_value = {
        'id': 'task1',
        'status': tasks.TaskStatus.FINISHED
    }
    task2 = mock.Mock()
    task2.id = 'task2'
    task2.to_dict.return_value = {
        'id': 'task2',
        'status': tasks.TaskStatus.RUNNING
    }
    self.mock_db.collection().stream.return_value = [task1, task2]

    job._load('test_job_1001')
    self.assertEqual(job.id, 'test_job_1001')
    for task in job.tasks:
      if task.id == 'task1':
        self.assertEqual(task.status, tasks.TaskStatus.FINISHED)
      if task.id == 'task2':
        self.assertEqual(task.status, tasks.TaskStatus.RUNNING)


if __name__ == '__main__':
  unittest.main()
