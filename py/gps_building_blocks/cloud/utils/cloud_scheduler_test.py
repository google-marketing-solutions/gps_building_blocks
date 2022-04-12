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
"""Tests for gps_building_blocks.cloud.utils.cloud_scheduler."""
from google.auth import credentials
from googleapiclient import errors

from absl.testing import absltest
from absl.testing import parameterized
from absl.testing.absltest import mock
from gps_building_blocks.cloud.utils import cloud_auth
from gps_building_blocks.cloud.utils import cloud_scheduler


class CloudSchedulerTest(parameterized.TestCase):

  def setUp(self):
    """Creates mock objects for googleapi client."""
    super(CloudSchedulerTest, self).setUp()
    self.addCleanup(mock.patch.stopall)
    self.project_id = 'project_id'
    self.location = 'us-central1'
    self.service_account_key_file = '/tmp/service_account_key.json'
    self.mock_get_credentials = mock.patch.object(
        cloud_auth, 'get_credentials', autospec=True).start()
    self.mock_build_service_client = mock.patch.object(
        cloud_auth, 'build_service_client', autospec=True).start()
    self.mock_client = mock.Mock()
    self.mock_credentials = mock.Mock(credentials.Credentials, autospec=True)
    self.mock_get_credentials.return_value = self.mock_credentials
    self.mock_build_service_client.return_value = self.mock_client
    self.fake_appengine_http_target = cloud_scheduler.AppEngineTarget(
        http_method='GET', relative_uri='/test', service='test')
    self.fake_http_target = cloud_scheduler.HttpTarget(
        http_method='POST',
        uri='https://www.google.com/',
        body='{}',
        headers={'Content-Type': 'application/json'},
        authorization_header=('my-fake-account@google.com', 'my-fake-scope'))
    self.scheduler = cloud_scheduler.CloudSchedulerUtils(
        project_id=self.project_id,
        location=self.location,
        service_account_key_file=self.service_account_key_file)

  @mock.patch.object(cloud_auth, 'build_impersonated_client', autospec=True)
  def test_client_initializes_with_impersonated_service_account(
      self, mock_impersonated_client):
    service_account_name = 'my-svc-account@project-id.iam.gserviceaccount.com'

    cloud_scheduler.CloudSchedulerUtils(
        project_id=self.project_id, service_account_name=service_account_name)

    mock_impersonated_client.assert_called_once_with('cloudscheduler',
                                                     service_account_name,
                                                     'v1beta1')

  def test_client_initializes_value_error(self):
    with self.assertRaises(ValueError):
      cloud_scheduler.CloudSchedulerUtils(project_id=self.project_id)

  def test_create_appengine_http_job(self):
    expected_job_name = 'created_job_name'
    mock_create_job = (
        self.mock_client.projects.return_value.locations.return_value.jobs
        .return_value.create)
    mock_create_job.return_value.execute.return_value.name = expected_job_name

    job_name = self.scheduler.create_appengine_http_job(
        name='my_job',
        description='unit test job',
        schedule='0 * * * *',
        target=self.fake_appengine_http_target)

    mock_create_job.assert_called_once_with(
        parent=f'projects/{self.project_id}/locations/{self.location}',
        body={
            'name': 'my_job',
            'description': 'unit test job',
            'schedule': '0 * * * *',
            'timeZone': 'GMT',
            'appEngineHttpTarget': {
                'httpMethod': 'GET',
                'appEngineRouting': {
                    'service': 'test'
                },
                'relativeUri': '/test'
            }
        })
    self.assertEqual(job_name, expected_job_name)

  def test_broken_create_appengine_job(self):
    with self.assertRaisesWithLiteralMatch(
        cloud_scheduler.Error,
        'Error occurred while creating job: <HttpError 500 "custom message">'):

      mock_http_response = mock.Mock(status=500, reason='custom message')
      mock_create_job = (
          self.mock_client.projects.return_value.locations.return_value.jobs
          .return_value.create)

      mock_create_job.side_effect = errors.HttpError(mock_http_response, b'')

      self.scheduler.create_appengine_http_job(
          name='my job',
          description='my description',
          schedule='0 * * * * ',
          target=self.fake_appengine_http_target)

  @parameterized.parameters(('oauthToken', {
      'serviceAccountEmail': 'my-fake-account@google.com',
      'scope': 'my-fake-scope'
  }), ('oidcToken', {
      'serviceAccountEmail': 'my-fake-account@google.com',
      'audience': 'my-fake-scope'
  }))
  def test_create_http_job(self, auth_type, expected_auth_header):
    expected_job_name = 'created_job_name'
    mock_create_job = (
        self.mock_client.projects.return_value.locations.return_value.jobs
        .return_value.create)
    mock_create_job.return_value.execute.return_value.name = expected_job_name

    self.fake_http_target.authorization_header_type = auth_type
    job_name = self.scheduler.create_http_job(
        name='my_job',
        description='unit test job',
        schedule='0 * * * *',
        target=self.fake_http_target)

    mock_create_job.assert_called_once_with(
        parent=f'projects/{self.project_id}/locations/{self.location}',
        body={
            'name': 'my_job',
            'description': 'unit test job',
            'schedule': '0 * * * *',
            'timeZone': 'GMT',
            'httpTarget': {
                'httpMethod': 'POST',
                'uri': 'https://www.google.com/',
                'headers': {
                    'Content-Type': 'application/json'
                },
                'body': '{}',
                auth_type: expected_auth_header
            }
        })
    self.assertEqual(job_name, expected_job_name)


if __name__ == '__main__':
  absltest.main()
