# python3
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

"""Tests for google3.corp.gtech.ads.data_catalyst.components.models.cc4d.operators.automl_tables_batch_prediction_operator."""

from absl.testing import absltest
from absl.testing.absltest import mock
from gps_building_blocks.airflow.hooks import automl_tables_hook
from gps_building_blocks.airflow.operators import automl_tables_batch_prediction_operator as automl_pred_op


class AutomlTablesBatchPredictionOperatorTest(absltest.TestCase):

  def setUp(self):
    super().setUp()

    # Mock out constructor to avoid Exceptions reading from Airflow env
    automl_tables_hook.AutoMLTablesHook.__init__ = mock.Mock(return_value=None)
    self.mock_predict = mock.Mock()
    automl_tables_hook.AutoMLTablesHook.batch_predict = self.mock_predict

    self.mock_input_path = mock.Mock(return_value='gs://input')
    (automl_pred_op.AutoMLTablesBatchPredictionOperator
     ._get_input_path) = self.mock_input_path
    self.mock_output_path = mock.Mock()
    (automl_pred_op.AutoMLTablesBatchPredictionOperator
     ._set_output_path) = self.mock_output_path

  def test_init_success(self):
    op = automl_pred_op.AutoMLTablesBatchPredictionOperator(
        task_id='test',
        model_id='my_test_model_id',
        input_key='input_key',
        output_key='output_key',
        output_path='gs://output')
    self.assertIsNotNone(op)

  def test_init_fail_no_input_path_or_input_key(self):
    with self.assertRaises(ValueError):
      automl_pred_op.AutoMLTablesBatchPredictionOperator(
          task_id='test',
          model_id='my_test_model_id',
          output_key='output_key',
          output_path='gs://output')

  def test_init_fail_both_input_path_and_input_key(self):
    with self.assertRaises(ValueError):
      automl_pred_op.AutoMLTablesBatchPredictionOperator(
          task_id='test',
          model_id='my_test_model_id',
          input_path='gs://input',
          input_key='input_key',
          output_key='output_key',
          output_path='gs://output')

  def test_execute_success_output_to_gs(self):
    self.mock_predict.return_value = {
        'done': True,
        'metadata': {
            'batchPredictDetails': {
                'outputInfo': {
                    'gcsOutputDirectory': 'gs://output_path'
                }
            }
        }
    }

    op = automl_pred_op.AutoMLTablesBatchPredictionOperator(
        task_id='test',
        model_id='my_test_model_id',
        input_path='gs://input',
        output_key='output_key',
        output_path='gs://output')
    op.execute(context={'foo': 'bar'})

    self.mock_predict.assert_called_with(
        model_id='my_test_model_id',
        input_path='gs://input',
        output_path='gs://output')
    self.mock_input_path.assert_called_with({'foo': 'bar'})
    self.mock_output_path.assert_called_with({'foo': 'bar'}, 'gs://output_path')

  def test_execute_success_output_to_bq(self):
    self.mock_predict.return_value = {
        'done': True,
        'metadata': {
            'batchPredictDetails': {
                'outputInfo': {
                    'bigqueryOutputDataset': 'bq://output_path'
                }
            }
        }
    }
    op = automl_pred_op.AutoMLTablesBatchPredictionOperator(
        task_id='test',
        model_id='my_test_model_id',
        input_path='gs://input',
        output_key='output_key',
        output_path='gs://output')

    op.execute(context={'foo': 'bar'})

    self.mock_predict.assert_called_with(
        model_id='my_test_model_id',
        input_path='gs://input',
        output_path='gs://output')
    self.mock_input_path.assert_called_with({'foo': 'bar'})
    self.mock_output_path.assert_called_with({'foo': 'bar'}, 'bq://output_path')

  def test_execute_raise_error_when_prediction_fail(self):
    self.mock_predict.return_value = {'done': True, 'error': 'error message'}
    op = automl_pred_op.AutoMLTablesBatchPredictionOperator(
        task_id='test',
        model_id='my_test_model_id',
        input_path='gs://input',
        output_key='output_key',
        output_path='gs://output')

    with self.assertRaises(RuntimeError):
      op.execute(context={'foo': 'bar'})

    self.mock_predict.assert_called_with(
        model_id='my_test_model_id',
        input_path='gs://input',
        output_path='gs://output')
    self.mock_input_path.assert_called_with({'foo': 'bar'})
    self.mock_output_path.assert_not_called()


if __name__ == '__main__':
  absltest.main()
