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

"""Tests for tcrm.operators.datastore_operator."""

import unittest
from unittest import mock

from airflow.contrib.hooks import gcp_api_base_hook

from gps_building_blocks.tcrm.hooks import monitoring_hook
from gps_building_blocks.tcrm.operators import data_connector_operator
from gps_building_blocks.tcrm.utils import blob
from gps_building_blocks.tcrm.utils import errors
from gps_building_blocks.tcrm.utils import hook_factory


def fake_events_generator(blobs):
  """Fake events generator."""
  for blb in blobs:
    yield blb


class DataConnectorOperatorTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.addCleanup(mock.patch.stopall)

    self.test_operator_kwargs = {'task_id': 'test_task_id',
                                 'tcrm_gcs_to_ga_schedule': '@once',
                                 'ga_tracking_id': 'UA-12345-67',
                                 'ga_base_params': {'v': '1'},
                                 'gcs_bucket': 'test_bucket',
                                 'gcs_prefix': 'test_dataset',
                                 'gcs_content_type': 'JSON',}

    self.mock_hook_factory_input = mock.patch.object(
        hook_factory, 'get_input_hook', autospec=True).start()
    self.mock_hook_factory_output = mock.patch.object(
        hook_factory, 'get_output_hook', autospec=True).start()

    self.original_gcp_hook_init = gcp_api_base_hook.GoogleCloudBaseHook.__init__
    gcp_api_base_hook.GoogleCloudBaseHook.__init__ = mock.MagicMock()

    self.original_monitoring_hook = monitoring_hook.MonitoringHook
    self.mock_monitoring_hook = mock.MagicMock()
    monitoring_hook.MonitoringHook = self.mock_monitoring_hook
    monitoring_hook.MonitoringHook.return_value = self.mock_monitoring_hook
    self.mock_generator = mock.MagicMock()
    self.mock_monitoring_hook.generate_processed_blobs_ranges = mock.MagicMock()
    (self.mock_monitoring_hook
     .generate_processed_blobs_ranges.return_value) = self.mock_generator

    self.dc_operator = data_connector_operator.DataConnectorOperator(
        dag_name='dag_name',
        input_hook=hook_factory.InputHookType.GOOGLE_CLOUD_STORAGE,
        output_hook=hook_factory.OutputHookType.GOOGLE_ANALYTICS,
        return_report=True,
        monitoring_dataset='test_dataset',
        monitoring_table='test_table',
        monitoring_bq_conn_id='test_monitoring_bq_conn_id',
        **self.test_operator_kwargs)

    self.dc_operator_disable_monitoring = (
        data_connector_operator.DataConnectorOperator(
            dag_name='dag_name',
            input_hook=hook_factory.InputHookType.GOOGLE_CLOUD_STORAGE,
            output_hook=hook_factory.OutputHookType.GOOGLE_ANALYTICS,
            return_report=True,
            enable_monitoring=False,
            monitoring_dataset='test_dataset',
            monitoring_table='test_table',
            monitoring_bq_conn_id='test_monitoring_bq_conn_id',
            **self.test_operator_kwargs))

    self.dc_operator_no_report = (
        data_connector_operator.DataConnectorOperator)(
            dag_name='dag_name',
            input_hook=hook_factory.InputHookType.GOOGLE_CLOUD_STORAGE,
            output_hook=hook_factory.OutputHookType.GOOGLE_ANALYTICS,
            monitoring_dataset='test_dataset',
            monitoring_table='test_table',
            monitoring_bq_conn_id='test_monitoring_bq_conn_id',
            **self.test_operator_kwargs)

    self.event = {
        'cid': '12345.67890',
        'ec': 'ClientID',
        'ea': 'PredictedPayer',
        'el': '20190423',
        'ev': 1,
        'z': '1558517072202080'
    }
    self.blob = blob.Blob(events=([self.event] * 2), location='blob')

  def tearDown(self):
    super().tearDown()
    gcp_api_base_hook.GoogleCloudBaseHook.__init__ = self.original_gcp_hook_init
    monitoring_hook.MonitoringHook = self.original_monitoring_hook

  def test_execute_appends_empty_reports_when_no_events_to_send(self):
    blb = blob.Blob(events=[], location='blob')
    (self.dc_operator.input_hook.events_blobs_generator.
     return_value) = fake_events_generator([blb] * 2)
    self.dc_operator.output_hook.send_events.return_value = blob.Blob(
        events=[], location='')

    reports = self.dc_operator.execute({})

    self.assertListEqual(reports, [[], []])

  def test_execute_appends_reports_after_sending_events(self):
    (self.dc_operator.input_hook.events_blobs_generator.
     return_value) = fake_events_generator([self.blob] * 2)
    (self.dc_operator.output_hook.send_events.
     return_value) = blob.Blob(events=[], location='', reports=([0], [1]))

    reports = self.dc_operator.execute({})

    self.assertListEqual(reports, [([0], [1]), ([0], [1])])

  def test_execute_returns_none_if_return_report_is_false(self):
    (self.dc_operator_no_report.input_hook.events_blobs_generator.
     return_value) = fake_events_generator([self.blob])
    (self.dc_operator_no_report.output_hook.send_events.
     return_value) = blob.Blob(events=[], location='', reports=([0, 1], []))

    result = self.dc_operator_no_report.execute({})

    self.assertIsNone(result)

  def test_execute_when_monitoring_is_enabled(self):
    (self.dc_operator.input_hook.events_blobs_generator.
     return_value) = fake_events_generator([self.blob] * 2)
    (self.dc_operator.output_hook.send_events.
     return_value) = blob.Blob(events=[], location='', reports=([0], [1]))

    self.dc_operator_no_report.execute({'test': 100})

    self.dc_operator.input_hook.events_blobs_generator.assert_called_with(
        processed_blobs_generator=self.mock_generator)
    self.mock_monitoring_hook.return_value.store_blob.assert_called()
    self.mock_monitoring_hook.return_value.store_events.assert_called()

  def test_execute_when_monitoring_is_disabled(self):
    (self.dc_operator.input_hook.events_blobs_generator.
     return_value) = fake_events_generator([self.blob] * 2)
    (self.dc_operator.output_hook.send_events.
     return_value) = blob.Blob(events=[], location='', reports=([0], [1]))

    self.dc_operator_disable_monitoring.execute({'test': 100})

    self.mock_monitoring_hook.return_value.store_blob.assert_not_called()
    self.mock_monitoring_hook.return_value.store_events.assert_not_called()

  def test_execute_when_is_retry_true(self):
    self.dc_operator_no_report.is_retry = True

    self.dc_operator_no_report.execute({})

    (self.mock_monitoring_hook.return_value.
     events_blobs_generator.assert_called_once())
    (self.dc_operator_no_report.input_hook.events_blobs_generator.
     return_value.assert_not_called())

  def test_execute_monitoring_does_not_use_default_bq_conn_id(self):
    self.test_operator_kwargs['bq_conn_id'] = 'test_bq_conn_id'
    with mock.patch('google3.third_party.gps_building_blocks.tcrm.hooks.'
                    'monitoring_hook.MonitoringHook', autospec=True) as mocker:
      data_connector_operator.DataConnectorOperator(
          dag_name='dag_name',
          input_hook=hook_factory.InputHookType.GOOGLE_CLOUD_STORAGE,
          output_hook=hook_factory.OutputHookType.GOOGLE_ANALYTICS,
          return_report=True, monitoring_dataset='test_dataset',
          monitoring_table='test_table',
          monitoring_bq_conn_id='test_monitoring_bq_conn_id',
          **self.test_operator_kwargs)
      mocker.assert_called_with(bq_conn_id='test_bq_conn_id')

  def test_execute_monitoring_use_default_bq_conn_id(self):
    with mock.patch('google3.third_party.gps_building_blocks.tcrm.hooks.'
                    'monitoring_hook.MonitoringHook', autospec=True) as mocker:
      data_connector_operator.DataConnectorOperator(
          dag_name='dag_name',
          input_hook=hook_factory.InputHookType.GOOGLE_CLOUD_STORAGE,
          output_hook=hook_factory.OutputHookType.GOOGLE_ANALYTICS,
          return_report=True, monitoring_dataset='test_dataset',
          monitoring_table='test_table',
          monitoring_bq_conn_id='test_monitoring_bq_conn_id',
          **self.test_operator_kwargs)
      mocker.assert_called_with(bq_conn_id='bigquery_default')

  def test_execute_monitoring_bad_values(self):
    with self.assertRaises(errors.MonitoringValueError):
      data_connector_operator.DataConnectorOperator(
          dag_name='dag_name',
          input_hook=hook_factory.InputHookType.GOOGLE_CLOUD_STORAGE,
          output_hook=hook_factory.OutputHookType.GOOGLE_ANALYTICS,
          return_report=True, monitoring_dataset='',
          **self.test_operator_kwargs)


if __name__ == '__main__':
  unittest.main()
