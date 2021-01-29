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

"""Operator for doing AutoML Tables batch predictions.

  Typical usage example:
    import AutoMLTablesBatchPredictionOperator
    op = AutoMLTablesBatchPredictionOperator(model_id='my_model_id',
                                             input_path='gs://path/to/input',
                                             output_path='gs://path/to/output',
                                             output_key='predict_output')
"""

from typing import Any, Mapping, Text
from airflow import models
from gps_building_blocks.airflow.hooks import automl_tables_hook


class AutoMLTablesBatchPredictionOperator(models.BaseOperator):
  """Operator for AutoML Tables predictions."""

  template_fields = ['input_path', 'output_path']

  def __init__(self,
               model_id: Text,
               output_path: Text,
               input_key: Text = None,
               input_path: Text = None,
               output_key: Text = None,
               conn_id: Text = 'google_cloud_default',
               **kwargs):
    """Constructor.

    Args:
      model_id: The model id for the AutoML model.
      output_path: The path to the prediction results. Supports either bq:// or
        gs:// paths.
      input_key: The key to get input path. Only specify either input key or
        input path.
      input_path: The path to input data. Supports either bq:// or gs:// paths.
      output_key: Write the prediction output path (either bq:// or gs://) with
        this key.
      conn_id: The connection id used by AutoML hooks to get authorization
        tokens.
      **kwargs: Keyword aruguments passed to base class constructor.
    """
    if input_key is None and input_path is None:
      raise ValueError('Either input_key or input_path should be specified')
    if input_key and input_path:
      raise ValueError('Only one of input key or input_path can be specified')

    self.input_key = input_key
    self.input_path = input_path
    self.output_path = output_path
    self.output_key = output_key
    self.model_id = model_id
    self.conn_id = conn_id

    super().__init__(**kwargs)

  def _get_input_path(self, context: Mapping[Text, Any]) -> Text:
    """Get input path.

    If input path is specified, return it directly, otherwise get input path
      from input key.

    Args:
      context: Airflow context.

    Returns:
      Path to input data.
    """
    if self.input_path:
      return self.input_path
    else:
      return self.xcom_pull(context=context, key=self.input_key)

  def _set_output_path(self, context: Mapping[Text, Any],
                       output_path: Text) -> Any:
    """Write output path info to output_key.

    Args:
      context: Airflow context.
      output_path: Path to prediction result.
    """
    if self.output_key:
      self.xcom_push(context=context, key=self.output_key, value=output_path)

  def _do_batch_prediction(self, input_path: Text, output_path: Text) -> Text:
    """Do batch prediction.

    Do AutoML batch prediction using data in input_path, write results to
      output_path(or with output prefix). Since the AutoML API may add
      timestamps to the output path specified by the user, the real output path
      must be extracted from the response.

    Args:
      input_path: The path to input data.
      output_path: The output path(for bq://) or output prefix(for gs://) passed
        to AutoML API to write prediction results.

    Returns:
      The output path for the prediction results.

    Raises:
      RuntimeError: When there are errors in the AutoML API response.
    """
    hook = automl_tables_hook.AutoMLTablesHook(gcp_conn_id=self.conn_id)
    resp = hook.batch_predict(model_id=self.model_id,
                              input_path=input_path,
                              output_path=output_path)
    if 'error' in resp:
      raise RuntimeError(f'Error in predictions {resp}')
    output_info = resp['metadata']['batchPredictDetails']['outputInfo']
    if 'gcsOutputDirectory' in output_info:
      return output_info['gcsOutputDirectory']
    elif 'bigqueryOutputDataset' in output_info:
      return output_info['bigqueryOutputDataset']
    else:
      raise RuntimeError(f'Output not found in prediction response: {resp}')

  def execute(self, context: Mapping[Text, Any]) -> Any:
    """Make AutoML Tables predictions.

    First call AutoML Tables hook to get predictions for input data, then
      write output path to the given output key.

    Args:
      context: Context provided by Airflow.
    """
    input_path = self._get_input_path(context)
    output_path = self._do_batch_prediction(input_path, self.output_path)
    self._set_output_path(context, output_path)
