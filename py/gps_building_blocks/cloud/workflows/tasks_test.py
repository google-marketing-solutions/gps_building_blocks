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

"""Tests for gps_building_blocks.cloud.workflows.tasks."""

import base64
import datetime
import json

from absl.testing import absltest
from absl.testing.absltest import mock
from gps_building_blocks.cloud.firestore import fake_firestore
from gps_building_blocks.cloud.workflows import futures
from gps_building_blocks.cloud.workflows import tasks


class TasksTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    self.db = fake_firestore.FakeFirestore()

    self.topic_path = 'MockTopicPath'
    self.max_parallel_tasks = 3
    self.mock_pubsub = mock.Mock()
    self.mock_pubsub.topic_path.return_value = self.topic_path

  def _define_job(self) -> tasks.Job:
    return tasks.Job(name='test_job',
                     project='test_project',
                     db=self.db,
                     pubsub=self.mock_pubsub,
                     max_parallel_tasks=self.max_parallel_tasks)

  def _define_job_with_two_dependent_tasks(self) -> tasks.Job:
    job = self._define_job()

    @job.task(task_id='task1')
    def task1(job, task):
      mark_unused(job, task)
      return 'result1'

    @job.task(task_id='task2', deps=['task1'])
    def task2(job, task):
      mark_unused(job, task)
      return 'result2'

    mark_unused(task1, task2)

    return job

  def _call_job_scheduler(self, job, scheduler):
    event = {'data': base64.b64encode(
        json.dumps({'id': job.id}).encode('utf-8'))}
    scheduler(event, {})

  def test_can_create_job(self):
    job = self._define_job()

    self.assertEqual(job.name, 'test_job')
    self.assertEqual(job.project, 'test_project')

  def test_can_create_tasks(self):
    job = self._define_job_with_two_dependent_tasks()

    task_ids = sorted([task.id for task in job.tasks])
    self.assertListEqual(task_ids, ['task1', 'task2'])

  def test_task_dependency(self):
    job = self._define_job()

    @job.task(task_id='task1')
    def task1(job, task):
      mark_unused(job, task)
      return 'result1'

    @job.task(task_id='task2', deps=['task1'])
    def task2(job, task):
      mark_unused(job, task)
      return 'result2'

    @job.task(task_id='task3', deps=['task1'])
    def task3(job, task):
      mark_unused(job, task)
      return 'result3'

    @job.task(task_id='task4', deps=['task2', 'task3'])
    def task4(job, task):
      mark_unused(job, task)
      return 'result4'

    mark_unused(task1, task2, task3, task4)

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
    self.assertEmpty(runnable_tasks)

    for task in job.tasks:
      if task.id == 'task3':
        task.status = tasks.TaskStatus.FINISHED

    # Finally, after task1-3 are finished, task4 is able to run
    runnable_tasks = [task.id for task in job._get_runnable_tasks()]
    self.assertListEqual(runnable_tasks, ['task4'])

  def test_start_job_should_create_db_entries(self):
    job = self._define_job_with_two_dependent_tasks()

    job.start()
    job_ref = (
        self.db.collection(tasks.Job.JOB_STATUS_COLLECTION).document(job.id))
    self.assertIsNotNone(job_ref)
    tasks_ref = job_ref.collection(tasks.Job.FIELD_TASKS)
    self.assertIsNotNone(tasks_ref)
    task1_ref = tasks_ref.document('task1')
    self.assertIsNotNone(task1_ref)

  def test_load_job_should_get_job_content_from_db(self):
    job_to_load = self._define_job_with_two_dependent_tasks()
    job_to_load.start()

    job = self._define_job_with_two_dependent_tasks()
    job._load(job_to_load.id)
    self.assertEqual(job.id, job_to_load.id)
    self.assertLen(job.tasks, 2)

  def test_schedule_successful_task_should_send_pubsub_message(self):
    job = self._define_job_with_two_dependent_tasks()
    scheduler = job.make_scheduler()
    job.start()

    message = json.dumps({'id': job.id}).encode('utf-8')
    call = [mock.call(self.topic_path, data=message)]
    # When start() is called, there should be #{max_parallel_tasks} pubsub
    # messages sent to pubsub.
    self.mock_pubsub.publish.assert_has_calls(call * self.max_parallel_tasks)
    self.assertEqual(self.mock_pubsub.publish.call_count,
                     self.max_parallel_tasks)
    self._call_job_scheduler(job, scheduler)
    # When task1 finishes, there should be another #{max_parallel_tasks} pubsub
    # messages sent to pubsub to trigger subsequent tasks.
    self.mock_pubsub.publish.assert_has_calls(
        call * (self.max_parallel_tasks*2))
    self.assertEqual(self.mock_pubsub.publish.call_count,
                     self.max_parallel_tasks * 2)

  def test_schedule_successful_tasks_should_set_task_statuses(self):
    job = self._define_job_with_two_dependent_tasks()
    scheduler = job.make_scheduler()
    job.start()

    self._call_job_scheduler(job, scheduler)
    # when job starts, the first task will be scheduled to run and returns
    # 'result1'
    tasks_ref = job._get_tasks_ref()
    task1 = tasks_ref.document('task1').get().to_dict()
    self.assertEqual(task1['status'], tasks.TaskStatus.FINISHED)
    self.assertEqual(task1['result'], 'result1')
    task2 = tasks_ref.document('task2').get().to_dict()
    self.assertEqual(task2['status'], tasks.TaskStatus.READY)

    # when scheduler is called again, task2 should be finished with result
    # 'result2'
    self._call_job_scheduler(job, scheduler)
    task2 = tasks_ref.document('task2').get().to_dict()
    self.assertEqual(task2['status'], tasks.TaskStatus.FINISHED)
    self.assertEqual(task2['result'], 'result2')

  def test_schedule_failed_tasks_should_raise_error(self):
    job = self._define_job()
    scheduler = job.make_scheduler()

    class MyTaskError(Exception):
      pass

    @job.task(task_id='task1')
    def task1(job, task):
      mark_unused(job, task)
      raise MyTaskError()

    with self.assertRaises(MyTaskError):
      job.start()
      self._call_job_scheduler(job, scheduler)

  def test_schedule_async_jobs(self):
    job = self._define_job()
    scheduler = job.make_scheduler()
    external_event_listener = job.make_external_event_listener()

    @job.task(task_id='task1')
    def task1(job, task):
      mark_unused(job, task)
      return futures.BigQueryFuture(trigger_id='test-bq-job-id')

    job.start()
    self._call_job_scheduler(job, scheduler)

    # When an asynchronous task finishes, a trigger object is created in db.
    triggers_ref = self.db.collection(tasks.Job.EVENT_TRIGGERS_COLLECTION)
    trigger = triggers_ref.document('test-bq-job-id').get().to_dict()
    self.assertEqual(trigger['task_id'], 'task1')
    self.assertEqual(trigger['job_id'], job.id)

    # Manually trigger a bq job finish log message
    bq_finish_event = {
        'protoPayload': {
            'status': {},
            'serviceData': {
                'jobCompletedEvent': {
                    'job': {
                        'jobName': {
                            'projectId': 'test-project',
                            'jobId': 'test-bq-job-id',
                        }
                    }
                }
            }
        },
        'resource': {
            'type': 'bigquery_resource'
        }
    }
    bq_finish_event_encoded = base64.b64encode(
        json.dumps(bq_finish_event).encode('utf-8'))

    external_event_listener({'data': bq_finish_event_encoded}, None)
    jobs_ref = self.db.collection(tasks.Job.JOB_STATUS_COLLECTION)
    task1 = jobs_ref.document(job.id).collection(
        tasks.Job.FIELD_TASKS).document('task1').get().to_dict()
    self.assertEqual(task1['status'], tasks.TaskStatus.FINISHED)

  def test_cleanup_should_delete_expired_jobs(self):
    jobs_ref = self.db.collection(tasks.Job.JOB_STATUS_COLLECTION)
    now = datetime.datetime.now()
    time1 = now - datetime.timedelta(days=29)
    fmt = '%Y-%m-%d-%H:%M:%S'
    jobs_ref.document('job1').set({
        'name': 'job1',
        'start_time': time1.strftime(fmt)
    })

    time2 = now - datetime.timedelta(days=31)
    fmt = '%Y-%m-%d-%H:%M:%S'
    jobs_ref.document('job2').set({
        'name': 'job2',
        'start_time': time2.strftime(fmt)
    })

    self.assertIn('job1', jobs_ref._data)
    self.assertIn('job2', jobs_ref._data)

    tasks.cleanup_expired_jobs(db=self.db, max_expire_days=30)
    self.assertIn('job1', jobs_ref._data)
    self.assertNotIn('job2', jobs_ref._data)


def mark_unused(*args):
  """Marks arguments as unused to avoid pylint warnings."""
  del args


if __name__ == '__main__':
  absltest.main()
