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

"""Tests for google3.third_party.gps_building_blocks.py.cloud.workflows.futures."""

import unittest

from gps_building_blocks.cloud.workflows import futures


class TasksTest(unittest.TestCase):

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


if __name__ == '__main__':
  unittest.main()
