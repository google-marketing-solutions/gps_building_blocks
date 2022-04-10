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

"""Custom hook for Google AutoML Tables API.

  Typical usage example:
    import AutoMLTablesHook
    hook = AutoMLTablesHook(gcp_conn_id='my_gcp_conn_id')
    hook.batch_predict(input_path='gs://bucket/path/to/data.csv',
                       output_path='gs://bucket/output-path-prefix',
                       model_id='my_model_id')
"""

import http
import logging
import time
import urllib
from airflow.contrib.hooks import gcp_api_base_hook
from google.auth.transport import requests


_AUTOML_BASE_URL = 'https://automl.googleapis.com/v1beta1'
_BATCH_PREDICT_RETRY_TIMES = 3


def retry_if_raises(times: int):
  """Returns a decorator to retry a function when exception is raised.

  Args:
    times: number of times to retry

  Returns:
    A decorator.
  """

  def dec(func):
    """Decorates a function to retry when exception is raised.

    Raises internal error after maximum number of retries.

    Args:
      func: Function to be wrapped.

    Returns:
      The wrapped function.
    """

    def wrapper(*args, **kwargs):
      for trial in range(times + 1):
        try:
          return func(*args, **kwargs)
        except RuntimeError as e:
          logging.exception('Failed for trial: %s', trial)
          if trial == times:
            raise e
    return wrapper

  return dec


class AutoMLTablesHook(gcp_api_base_hook.GoogleCloudBaseHook):
  """Hook to call AutoML Tables API."""

  def _build_batch_predict_payload(self, input_path: str, output_path: str):
    """Convert input/output paths into batch predict json payload.

    Args:
      input_path: See definition in batch_predict.
      output_path: See definition in batch_predict.

    Returns:
      A json dictionary of input/output config.

    Raises:
      ValueError when input path or output path are not valid.
    """
    if ',' in input_path:
      input_uris = [path.strip() for path in input_path.split(',')]
      for input_uri in input_uris:
        parse_res = urllib.parse.urlparse(input_uri)
        if parse_res.scheme != 'gs':
          raise ValueError('Comma separated paths only valid for Cloud Storage')
      input_config = {'gcsSource': {'inputUris': input_uris}}
    else:
      parse_res = urllib.parse.urlparse(input_path)
      if parse_res.scheme == 'bq':
        input_config = {'bigquerySource': {'input_uri': input_path}}
      elif parse_res.scheme == 'gs':
        input_config = {'gcsSource': {'inputUris': [input_path]}}
      else:
        raise ValueError(f'Unsupported scheme for input path: {input_path}')

    parse_res = urllib.parse.urlparse(output_path)
    if parse_res.scheme == 'bq':
      output_config = {'bigqueryDestination': {'outputUri': output_path}}
    elif parse_res.scheme == 'gs':
      output_config = {'gcsDestination': {'outputUriPrefix': output_path}}
    else:
      raise ValueError(f'Unsupported scheme for output path: {output_path}')

    return {'inputConfig': input_config, 'outputConfig': output_config}

  def _get_authorized_session(self):
    """Get an authorized HTTP session for calling AutoML API.

    Returns:
      An AuthorizedSession object.
    """
    # Get credentials from airflow environment using the base hook
    credentials = self._get_credentials()
    return requests.AuthorizedSession(credentials=credentials)

  @retry_if_raises(times=_BATCH_PREDICT_RETRY_TIMES)
  def batch_predict(self,
                    model_id: str,
                    input_path: str,
                    output_path: str,
                    compute_region='us-central1',
                    prediction_timeout=3600,
                    poll_wait_time=60):
    """Call AutoML batch prediction API and wait for prediction results.

    It first sends a prediction request to AutoML API, then poll its status
    every `poll_wait_time` seconds. It returns when the prediction task is
    finished, or raises an error when the total time polling exceeds
    prediction_timeout.

    See AutoML Tables API documentation for more information:
    https://cloud.google.com/automl-tables/docs/predict-batch

    Args:
      model_id: AutoML model id.
      input_path: Input path for training data. Supports both bq:// and
        gs:// paths. Use ',' to separate multiple cloud storage paths. Note:
        comma-separated paths is ONLY valid for cloud storage paths.
      output_path: Output path for prediction result. Supports both bq://
        and gs:// paths.
      compute_region: Compute region. Currently only 'us-central1' is supported.
      prediction_timeout: Maximum time(seconds) to wait for AutoML prediction to
        complete.
      poll_wait_time: time in seconds to sleep between polling predict operation
        status.

    Returns:
      Prediction results json dictionary from AutoML response. See:
      https://cloud.google.com/automl-tables/docs/reference/rest/v1beta1/projects.locations.operations#Operation

      Sample response:
      {
        "name": ...,
        "done": true,
        "metadata": {
          "batchPredictDetails": {
            "inputConfig": {
              ...
            },
            "outputInfo": {
              "gcsOutputDirectory": "gs://output-dir" |
              "bigqueryOutputDataset": "bq://output-dataset"
            }
          }
        }
      }

    Raises:
      RuntimeError: When errors ocurred calling AutoML API, or timeout waiting
        for prediction to complete.
    """
    # Submit batch prediction task
    session = self._get_authorized_session()
    payload = self._build_batch_predict_payload(input_path, output_path)
    url = (f'{_AUTOML_BASE_URL}/projects/{self.project_id}'
           f'/locations/{compute_region}/models/{model_id}:batchPredict')
    response = session.post(url, json=payload)
    if response.status_code != http.HTTPStatus.OK:
      raise RuntimeError('Error calling AutoML API status='
                         f'{response.status_code} message={response.text}')
    res = response.json()
    if 'error' in res:
      raise RuntimeError(f'Error submitting prediction request {res["error"]}')
    self.log.info('batch predict submit result: %s', res)
    # AutoML generates an op name for current prediction, which can be used
    # to query its status
    operation_name = res['name']

    # Wait for prediction results
    start_time = time.time()
    while (time.time() - start_time) < prediction_timeout:
      url = f'{_AUTOML_BASE_URL}/{operation_name}'
      response = session.get(url)
      if response.status_code != http.HTTPStatus.OK:
        raise RuntimeError('Error waiting for AutoML prediction status='
                           f'{response.status_code} message={response.text}')
      res = response.json()
      self.log.info('batch predict status: %s', res)
      if res.get('done', False):
        if 'error' in res:
          raise RuntimeError(f'Error in prediction result {res["error"]}')
        return res
      time.sleep(poll_wait_time)

    time_wait = time.time() - start_time
    raise RuntimeError(f'Timeout waiting {time_wait}s for AutoML prediction')
