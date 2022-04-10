# coding=utf-8
# Copyright 2021 Google LLC.
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

"""Tests for google3.corp.gtech.ads.data_catalyst.components.models.cc4d.hooks.automl_tables_hook."""

from absl.testing import absltest
from absl.testing.absltest import mock
from gps_building_blocks.airflow.hooks import automl_tables_hook


class AutoMLTablesHookTest(absltest.TestCase):

  def setUp(self):
    """Set up mocks for hook test."""
    super().setUp()
    # Mock constructor to avoid super class init Exceptions
    automl_tables_hook.AutoMLTablesHook.__init__ = mock.Mock(return_value=None)
    # Mock project id
    automl_tables_hook.AutoMLTablesHook.project_id = mock.PropertyMock(
        return_value='test_project')

    # Mock session
    mock_session = mock.Mock()
    automl_tables_hook.AutoMLTablesHook._get_authorized_session = mock_session
    self.mock_get = mock_session.return_value.get
    self.mock_post = mock_session.return_value.post

  def test_batch_predict_success(self):
    """Test for a successful batch prediction."""
    # Predict response
    self.mock_post.return_value.json.return_value = {
        'name': 'test_operation_name',
        'metadata': {}
    }
    self.mock_post.return_value.status_code = 200

    # Query status response
    self.mock_get.return_value.json.side_effect = [
        {'done': False, 'metadata': {}},
        {'done': False, 'metadata': {}},
        {'done': True,
         'metadata': {
             'batchPredictDetails': {
                 'outputInfo': {'gcsOutputDirectory': 'gs://outputfile'}}}},
    ]
    self.mock_get.return_value.status_code = 200

    hook = automl_tables_hook.AutoMLTablesHook()
    # In unit tests we do not want to sleep between polling
    pred_result = hook.batch_predict(
        input_path='gs://input',
        output_path='gs://output',
        model_id='test_model_id',
        compute_region='test_region',
        poll_wait_time=0)
    self.assertEqual(pred_result['done'], True)
    self.mock_get.assert_called_with(
        'https://automl.googleapis.com/v1beta1/test_operation_name')
    self.mock_post.assert_called_with(
        ('https://automl.googleapis.com/v1beta1/projects/test_project'
         '/locations/test_region/models/test_model_id:batchPredict'),
        json={'inputConfig': {'gcsSource': {'inputUris': ['gs://input']}},
              'outputConfig': {'gcsDestination':
                                   {'outputUriPrefix': 'gs://output'}}}
    )

  def test_batch_predict_fail(self):
    """Test for a batch prediction which fails when submitting request."""
    self.mock_post.return_value.status_code = 400
    self.mock_post.return_value.text = 'Bad Request'

    hook = automl_tables_hook.AutoMLTablesHook()
    # In unit tests we do not want to sleep between polling
    with self.assertRaises(RuntimeError) as cm:
      hook.batch_predict(
          input_path='gs://input',
          output_path='gs://output',
          compute_region='test_region',
          model_id='test_model_id',
          poll_wait_time=0)

    self.assertIn('Error calling AutoML API', cm.exception.args[0])

  def test_batch_predict_fail_wait(self):
    """Test for a batch prediction which fails when polling status."""
    self.mock_post.return_value.json.return_value = {
        'name': 'test_operation_name',
        'metadata': {}
    }
    self.mock_post.return_value.status_code = 200

    self.mock_get.return_value.status_code = 400
    self.mock_get.return_value.text = 'Bad Request'

    hook = automl_tables_hook.AutoMLTablesHook()
    # In unit tests we do not want to sleep between polling
    with self.assertRaises(RuntimeError) as cm:
      hook.batch_predict(
          input_path='gs://input',
          output_path='gs://output',
          model_id='test_model_id',
          compute_region='test_region',
          poll_wait_time=0)

    self.assertIn('Error waiting for AutoML', cm.exception.args[0])

  def test_batch_predict_fail_error(self):
    """Test for a batch prediction which fails with an error in response."""
    # Predict response
    self.mock_post.return_value.json.return_value = {
        'name': 'test_operation_name',
        'metadata': {}
    }
    self.mock_post.return_value.status_code = 200

    # Query status response
    self.mock_get.return_value.json.return_value = {'done': True,
                                                    'error': 'error message'}
    self.mock_get.return_value.status_code = 200

    hook = automl_tables_hook.AutoMLTablesHook()
    # In unit tests we do not want to sleep between polling
    with self.assertRaises(RuntimeError) as cm:
      hook.batch_predict(
          input_path='gs://input',
          output_path='gs://output',
          model_id='test_model_id',
          compute_region='test_region',
          poll_wait_time=0)

    self.assertIn('Error in prediction result', cm.exception.args[0])

  def test_batch_predict_retry(self):
    """Test for a retried prediction.

    Test for a batch prediction which fails for the first time, and after
    retry succeeds.
    """
    # Predict response
    self.mock_post.side_effect = [
        mock.Mock(status_code=400, text='Bad Request'),
        mock.Mock(status_code=200, json=mock.Mock(return_value={
            'name': 'test_operation_name',
            'metadata': {}
        })),
    ]

    # Query status response
    self.mock_get.return_value.json.side_effect = [
        {'done': False, 'metadata': {}},
        {'done': False, 'metadata': {}},
        {'done': True,
         'metadata': {
             'batchPredictDetails': {
                 'outputInfo': {'gcsOutputDirectory': 'gs://outputfile'}}}},
    ]
    self.mock_get.return_value.status_code = 200

    hook = automl_tables_hook.AutoMLTablesHook()
    # In unit tests we do not want to sleep between polling
    pred_result = hook.batch_predict(
        input_path='gs://input',
        output_path='gs://output',
        model_id='test_model_id',
        compute_region='test_region',
        poll_wait_time=0)
    self.assertEqual(pred_result['done'], True)

  def test_batch_predict_timeout(self):
    """Test for a batch predict which timeouts."""
    # predict response
    self.mock_post.return_value.json.return_value = {
        'name': 'test_operation_name',
        'metadata': {}
    }
    self.mock_post.return_value.status_code = 200

    # Query status response
    self.mock_get.return_value.json.side_effect = [
        {'done': False, 'metadata': {}},
        {'done': False, 'metadata': {}},
        {'done': True,
         'metadata': {
             'batchPredictDetails': {
                 'outputInfo': {'gcsOutputDirectory': 'gs://outputfile'}}}},
    ]
    self.mock_get.return_value.status_code = 200

    # Set timeout to 0 to ensure the request timeout
    hook = automl_tables_hook.AutoMLTablesHook()
    with self.assertRaises(RuntimeError) as cm:
      hook.batch_predict(
          input_path='gs://input',
          output_path='gs://output',
          prediction_timeout=0,
          model_id='test_model_id',
          compute_region='test_region',
          poll_wait_time=0)

    self.assertIn('Timeout', cm.exception.args[0])

  def test_build_predict_payload_gs2gs(self):
    """Test for building predict request payload from gs->gs."""
    hook = automl_tables_hook.AutoMLTablesHook()
    p = hook._build_batch_predict_payload('gs://input', 'gs://output')
    self.assertEqual(
        p, {
            'inputConfig': {
                'gcsSource': {
                    'inputUris': ['gs://input']
                }
            },
            'outputConfig': {
                'gcsDestination': {
                    'outputUriPrefix': 'gs://output'
                }
            }
        })

  def test_build_predict_payload_gs2gs_multi_inputs(self):
    hook = automl_tables_hook.AutoMLTablesHook()
    p = hook._build_batch_predict_payload('gs://input1, gs://input2',
                                          'gs://output')
    self.assertEqual(
        p, {
            'inputConfig': {
                'gcsSource': {
                    'inputUris': ['gs://input1', 'gs://input2']
                }
            },
            'outputConfig': {
                'gcsDestination': {
                    'outputUriPrefix': 'gs://output'
                }
            }
        })

  def test_build_predict_payload_gs2bq(self):
    hook = automl_tables_hook.AutoMLTablesHook()
    p = hook._build_batch_predict_payload('gs://input1, gs://input2',
                                          'bq://output')
    self.assertEqual(
        p, {
            'inputConfig': {
                'gcsSource': {
                    'inputUris': ['gs://input1', 'gs://input2']
                }
            },
            'outputConfig': {
                'bigqueryDestination': {
                    'outputUri': 'bq://output'
                }
            }
        })

  def test_build_predict_payload_bq2gs(self):
    hook = automl_tables_hook.AutoMLTablesHook()
    p = hook._build_batch_predict_payload('bq://input', 'gs://output')
    self.assertEqual(
        p, {
            'inputConfig': {
                'bigquerySource': {
                    'input_uri': 'bq://input'
                }
            },
            'outputConfig': {
                'gcsDestination': {
                    'outputUriPrefix': 'gs://output'
                }
            }
        })

  def test_build_predict_payload_bq2bq(self):
    hook = automl_tables_hook.AutoMLTablesHook()
    p = hook._build_batch_predict_payload('bq://input', 'bq://output')
    self.assertEqual(
        p, {
            'inputConfig': {
                'bigquerySource': {
                    'input_uri': 'bq://input'
                }
            },
            'outputConfig': {
                'bigqueryDestination': {
                    'outputUri': 'bq://output'
                }
            }
        })

  def test_build_predict_payload_invalid_uris(self):
    hook = automl_tables_hook.AutoMLTablesHook()
    with self.assertRaises(ValueError):
      hook._build_batch_predict_payload('http://input', 'bq://output')

  def test_build_predict_payload_invalid_multi_paths(self):
    hook = automl_tables_hook.AutoMLTablesHook()
    # multiple paths only supported for cloud storage inputs
    with self.assertRaises(ValueError):
      hook._build_batch_predict_payload('bq://input1, bq://input2',
                                        'bq://output')

if __name__ == '__main__':
  absltest.main()
