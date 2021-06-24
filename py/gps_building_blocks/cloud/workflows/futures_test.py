# python3
# coding=utf-8
# Copyright 2020 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for gps_building_blocks.cloud.workflows.futures."""

import urllib

import google.auth
from googleapiclient import discovery

from absl.testing import absltest
from absl.testing.absltest import mock
from gps_building_blocks.cloud.firestore import fake_firestore
from gps_building_blocks.cloud.workflows import futures


class TasksTest(absltest.TestCase):

  def setUp(self):
    super().setUp()

    self.addCleanup(mock.patch.stopall)

    mock_auth = mock.patch.object(google.auth, 'default', autospec=True).start()
    mock_auth.return_value = (None, 'test_project')

    self.mock_api = mock.Mock()
    mock_discovery = mock.patch.object(
        discovery, 'build', autospec=True).start()
    mock_discovery.return_value = self.mock_api

    self.db = fake_firestore.FakeFirestore()

  def test_remote_function_future_should_parse_generic_success_logs(self):
    # a fake generic message for job complete
    generic_message = {
        'status': {
            'code': 0,
            'message': 'test success message'
        },
        'resource': {
            'type': 'remote_function_resource',
            'labels': {
                'job_id': 'generic-job-id'
            }
        }
    }
    result = futures.RemoteFunctionFuture.handle_message(generic_message)
    self.assertTrue(result.is_success)
    self.assertEqual(result.trigger_id, 'generic-job-id')

  def test_remote_function_future_should_parse_generic_fail_logs(self):
    # a fake generic message for job complete
    generic_message = {
        'status': {
            'code': 1,
            'message': 'test error message'
        },
        'resource': {
            'type': 'remote_function_resource',
            'labels': {
                'job_id': 'generic-job-id'
            }
        }
    }
    result = futures.RemoteFunctionFuture.handle_message(generic_message)
    self.assertFalse(result.is_success)
    self.assertEqual(result.trigger_id, 'generic-job-id')
    self.assertEqual(result.error, 'test error message')

  def test_remote_function_future_should_parse_invalid_message(self):
    # a fake generic message for job complete
    invalid_message = {
        'status': {
            'code': 1,
            'message': 'test error message'
        }
    }
    result = futures.RemoteFunctionFuture.handle_message(invalid_message)
    self.assertIsNone(result)

  def test_bq_future_should_parse_bq_success_logs(self):
    # a fake bq message for job complete
    bq_message = {
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

    result = futures.BigQueryFuture.handle_message(bq_message)
    self.assertTrue(result.is_success)
    self.assertEqual(result.trigger_id, 'test-bq-job-id')

  def test_bq_future_should_parse_bq_fail_logs(self):
    # a fake bq message for job complete with failed status
    bq_message = {
        'protoPayload': {
            'status': {
                'code': 1,
                'message': 'test error message'
            },
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

    result = futures.BigQueryFuture.handle_message(bq_message)
    self.assertFalse(result.is_success)
    self.assertEqual(result.trigger_id, 'test-bq-job-id')
    self.assertEqual(result.error, 'test error message')

  def test_dataflow_future_should_parse_dataflow_success_logs(self):
    message = {
        'textPayload': 'Worker pool stopped.',
        'resource': {
            'type': 'dataflow_step',
            'labels': {
                'job_name': 'my_dataflow_job',
                'region': 'us-central1',
                'job_id': 'df_job_id'
            }
        }
    }

    self.mock_api.projects().locations().jobs().get().execute.return_value = {
        'currentState': 'JOB_STATE_DONE'
    }

    result = futures.DataFlowFuture.handle_message(message)
    self.assertTrue(result.is_success)
    self.assertEqual(result.trigger_id, 'df_job_id')

  def test_dataflow_future_should_parse_dataflow_fail_logs(self):
    message = {
        'textPayload': 'Worker pool stopped.',
        'resource': {
            'type': 'dataflow_step',
            'labels': {
                'job_name': 'my_dataflow_job',
                'region': 'us-central1',
                'job_id': 'df_job_id'
            }
        }
    }

    self.mock_api.projects().locations().jobs().get().execute.return_value = {
        'currentState': 'JOB_STATE_FAILED'
    }

    result = futures.DataFlowFuture.handle_message(message)
    self.assertFalse(result.is_success)
    self.assertEqual(result.trigger_id, 'df_job_id')

  def test_gcs_future_should_parse_gcs_messages(self):
    message = {
        'function_flow_event_type': 'gcs_path_create',
        'path_prefix': 'gs://test-bucket/test-path'
    }

    result = futures.GCSFuture.handle_message(message)
    self.assertEqual(result.result, 'gs://test-bucket/test-path')

  def test_gcs_poller_should_register_deregister_paths(self):
    path = 'gs://test-bucket/test-path'
    futures.GCSPoller.register_path_prefix(path, self.db)
    quoted_path = urllib.parse.quote(path, safe='')
    self.assertIn(quoted_path, self.db._data['GCSWatches'])
    futures.GCSPoller.deregister_path_prefix(path, self.db)
    self.assertNotIn(quoted_path, self.db._data['GCSWatches'])


if __name__ == '__main__':
  absltest.main()
