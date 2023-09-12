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

"""Tests for py.airflow.operators.datastore_operator."""

from absl.testing import absltest
from absl.testing.absltest import mock
from gps_building_blocks.airflow.hooks import ga_hook
from gps_building_blocks.airflow.hooks import gcs_hook
from gps_building_blocks.airflow.operators import data_connector_operator
from gps_building_blocks.airflow.utils import blob


def fake_events_generator(blobs):
  """Fake events generator."""
  for blb in blobs:
    yield blb


class DataConnectorOperatorTest(absltest.TestCase):

  def setUp(self):
    super(DataConnectorOperatorTest, self).setUp()
    self.addCleanup(mock.patch.stopall)

    self.mock_gcs_hook = mock.patch.object(
        gcs_hook, 'GoogleCloudStorageHook', autospec=True).start()

    self.mock_ga_hook = mock.patch.object(
        ga_hook, 'GoogleAnalyticsHook', autospec=True).start()

    self.test_operator_kwargs = {'task_id': 'test_task_id'}
    self.dc_operator = data_connector_operator.DataConnectorOperator(
        input_hook=self.mock_gcs_hook, output_hook=self.mock_ga_hook,
        return_report=True, **self.test_operator_kwargs)
    self.dc_operator_no_report = data_connector_operator.DataConnectorOperator(
        input_hook=self.mock_gcs_hook, output_hook=self.mock_ga_hook,
        **self.test_operator_kwargs)

    self.event = {
        'cid': '12345.67890',
        'ec': 'ClientID',
        'ea': 'PredictedPayer',
        'el': '20190423',
        'ev': 1,
        'z': '1558517072202080'
    }
    self.blob = blob.Blob(events=([self.event] * 2), blob_id='id',
                          platform='GCS', source='bucket', location='blob')

  def test_execute_appends_empty_reports_when_no_events_to_send(self):
    blb = blob.Blob(events=[], blob_id='id', platform='GCS', source='bucket',
                    location='blob')
    (self.dc_operator.input_hook.events_blobs_generator.
     return_value) = fake_events_generator([blb] * 2)

    reports = self.dc_operator.execute({})

    self.assertListEqual(reports, [(), ()])

  def test_execute_appends_reports_after_sending_events(self):
    (self.dc_operator.input_hook.events_blobs_generator.
     return_value) = fake_events_generator([self.blob] * 2)
    (self.dc_operator.output_hook.send_events.
     return_value) = ([0], [1])

    reports = self.dc_operator.execute({})

    self.assertListEqual(reports, [([0], [1]), ([0], [1])])

  def test_execute_sets_blob_unsent_events_indexes(self):
    (self.dc_operator.input_hook.events_blobs_generator.
     return_value) = fake_events_generator([self.blob])
    (self.dc_operator.output_hook.send_events.
     return_value) = ([0], [1])

    self.dc_operator.execute({})

    self.assertListEqual(self.blob.unsent_events_indexes, [1])

  def test_execute_sets_blob_processed_status(self):
    (self.dc_operator.input_hook.events_blobs_generator.
     return_value) = fake_events_generator([self.blob])
    (self.dc_operator.output_hook.send_events.
     return_value) = ([0, 1], [])

    self.dc_operator.execute({})

    self.assertEqual(self.blob.status, blob.BlobStatus.PROCESSED)

  def test_execute_returns_none_if_return_report_is_false(self):
    (self.dc_operator_no_report.input_hook.events_blobs_generator.
     return_value) = fake_events_generator([self.blob])
    (self.dc_operator_no_report.output_hook.send_events.
     return_value) = ([0, 1], [])

    result = self.dc_operator_no_report.execute({})

    self.assertIsNone(result)

  def test_execute_sets_blob_error_status_and_description(self):
    (self.dc_operator.input_hook.events_blobs_generator.
     return_value) = fake_events_generator([self.blob])
    (self.dc_operator.output_hook.send_events.
     return_value) = ([0], [1])

    self.dc_operator.execute({})

    self.assertListEqual([self.blob.status, self.blob.status_desc],
                         [blob.BlobStatus.ERROR,
                          'Error: There are some unsent events in this blob'])

if __name__ == '__main__':
  absltest.main()
