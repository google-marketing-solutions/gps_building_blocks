# Lint as: python3
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for gps_building_blocks.cloud.utils.utils."""

import time

from googleapiclient import errors
from googleapiclient import http

from absl.testing import absltest
from absl.testing import parameterized
from absl.testing.absltest import mock
from gps_building_blocks.cloud.utils import utils


class UtilsTest(parameterized.TestCase):

  def setUp(self):
    super(UtilsTest, self).setUp()
    self.addCleanup(mock.patch.stopall)
    self.operation_not_completed = {
        'name': 'projects/my-proj/locations/us-central1/operations/'
                '11111111-aaaa-2222-bbbb-111111111111',
        'done': False,
        'error': None
    }
    self.operation_completed = {
        'name': 'projects/my-proj/locations/us-central1/operations/'
                '22222222-aaaa-2222-bbbb-111111111111',
        'done': True,
        'error': None
    }
    self.operation_failed = {
        'name': 'projects/my-proj/locations/us-central1/operations/'
                '33333333-aaaa-2222-bbbb-111111111111',
        'done': False,
        'error': {
            'message': 'Timeout occurred!'
        }
    }
    self.sleep_mock = mock.patch.object(time, 'sleep', autospec=True).start()
    self.mock_client = mock.Mock()

  @parameterized.named_parameters(
      ('too_many_requests', 429, True),
      ('internal_server_error', 500, True),
      ('service_unavailable', 503, True),
      ('not_found', 400, False),
  )
  def test_is_retriable_http_error(self, status_code, is_retried):
    error = errors.HttpError(mock.MagicMock(status=status_code), b'')

    is_retriable_http_error = utils._is_retriable_http_error(error)

    self.assertEqual(is_retriable_http_error, is_retried)

  def test_execute_request(self):
    mock_request = mock.Mock(http.HttpRequest)
    utils.execute_request(mock_request)

    mock_request.execute.assert_called_once()

  def test_execute_request_retries_on_service_unavailable_http_error(self):
    mock_request = mock.Mock(http.HttpRequest)
    content = b''
    error = errors.HttpError(mock.MagicMock(status=503), content)
    mock_request.execute.side_effect = [error, None]

    utils.execute_request(mock_request)

    self.assertEqual(mock_request.execute.call_count, 2)

  def test_wait_for_completion_of_operation(self):
    mock_get_operation = (
        self.mock_client.get.return_value.execute)
    mock_get_operation.side_effect = [
        self.operation_not_completed, self.operation_not_completed,
        self.operation_completed
    ]

    utils.wait_for_operation(self.mock_client, self.operation_not_completed)

    self.assertEqual(3, mock_get_operation.call_count)
    self.assertEqual(2, self.sleep_mock.call_count)

  def test_wait_for_completion_of_operation_handles_errors(self):
    mock_get_operation = (
        self.mock_client.get.return_value.execute)
    mock_get_operation.side_effect = [
        self.operation_not_completed, self.operation_not_completed,
        self.operation_failed
    ]

    with self.assertRaises(utils.Error):
      utils.wait_for_operation(self.mock_client, self.operation_not_completed)

    self.assertEqual(3, mock_get_operation.call_count)


if __name__ == '__main__':
  absltest.main()
