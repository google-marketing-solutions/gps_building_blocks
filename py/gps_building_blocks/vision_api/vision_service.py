# Copyright 2023 Google LLC
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

"""Vision service that provides access to vision api for getting insights from an image."""

import abc
import json
import time
import traceback
from typing import Any

from absl import logging
from google.cloud import storage
from googleapiclient import discovery as cloud_discovery
from googleapiclient import errors as google_api_errors
import pandas as pd


class VisionApiInterface(abc.ABC):
  """Interface for vision service."""

  @abc.abstractmethod
  def run_async_batch_annotate_images(
      self, batch_size: int, time_to_sleep: int
  ) -> None:
    """Annotate images in a batch mode asynchronously.

    Args:
      batch_size: The max number of responses to output in each JSON file.
      time_to_sleep: Time to wait in seconds for checking status.
    """


class ExternalVisionService(VisionApiInterface):
  """External vision api service to annotate images.

  Typical usage example:

    external_vision_service = ExternalVisionService(
        input_gcs_uri='<your-gcs-bucket-uri>',
        output_gcs_uri='<your-gcs-bucket-uri>',
        features={'feature_name': <max_num_response>, 'feature_name':
        <max_num_response>},
    )
    external_vision_service.run_async_batch_annotate_images(
        batch_size=10, time_to_sleep=5)

  Attributes:
    input_gcs_uri: Input GCS uri which contains images.
    output_gcs_uri: Output GCS uri to store responses.
    features: A dictionary of feature and max results.
    vision_service: Cloud vision api service object.
  """

  def __init__(
      self, input_gcs_uri: str, output_gcs_uri: str, features: dict[str, int]
  ) -> None:
    """Initializes the instance with input path, output path and features.

    Args:
      input_gcs_uri: Input GCS uri which contains images.
      output_gcs_uri: Output GCS uri to store responses.
      features: A dictionary of feature and max results.
    """
    self._input_gcs_uri = input_gcs_uri
    self._output_gcs_uri = output_gcs_uri
    self._features = features
    self._vision_service = cloud_discovery.build('vision', 'v1')
    self._storage_client = storage.Client()

  def _get_bucket_name(self, bucket_uri: str) -> str:
    """Get bucket name from uri.

    Args:
      bucket_uri: GCS bucket uri.

    Returns:
      GCS bucket name.
    """
    return bucket_uri.split('/')[2]

  def _get_all_images(self, images_bucket_uri: str) -> list[str]:
    """Get a list of all image uris from GCS.

    Args:
      images_bucket_uri: GCS bucket name for images.

    Returns:
      List of gcs uris for all the images.
    """
    images_bucket_name = self._get_bucket_name(images_bucket_uri)
    bucket = self._storage_client.get_bucket(images_bucket_name)

    # Lists objects with the given prefix.
    blob_list = list(bucket.list_blobs())

    return [f'{images_bucket_uri}/{blob.name}' for blob in blob_list]

  def _add_features(self, features: dict[str, int]) -> list[dict[str, Any]]:
    """Add features for image annotation.

    Args:
      features: A dictionary conatins feature name and max results value.

    Returns:
      List of feature object required to do batch annotation.
    """
    return [
        {'type': feature, 'maxResults': max_result}
        for feature, max_result in features.items()
    ]

  def _generate_request_element(
      self, image_uri: str, features: dict[str, int]
  ) -> dict[str, Any]:
    """Generates request element for each image.

    Args:
        image_uri: URI of an image.
        features: A dictionary conatins feature name and max results value.

    Returns:
        Element containing image and features.
    """
    source = {'imageUri': image_uri}
    image = {'source': source}
    features = self._add_features(features=features)
    return {'image': image, 'features': features}

  def _create_requests(
      self,
      uris: list[str], features: dict[str, int]
  ) -> list[dict[str, Any]]:
    """Create collection of all request elements.

    Args:
      uris: List of all GCS image uris.
      features: A dictionary conatins feature name and max results value.

    Returns:
      List of all request elements.
    """
    all_requests = []

    for uri in uris:
      elem = self._generate_request_element(uri, features)
      all_requests.append(elem)

    return all_requests

  def _async_batch_annotate_images(
      self,
      img_requests: list[dict[str, str]],
      batch_size: int,
      time_to_sleep: int,
      output_uri: str,
  ) -> None:
    """Async annotation of images in batch.

    Args:
      img_requests: List of all the image requests.
      batch_size: The max number of responses to output in each JSON file.
      time_to_sleep: Time to wait in seconds for checking status.
      output_uri: GCS bucket to store responses.
    """
    completed = 'done'
    if not output_uri.endswith('/'):
      output_uri = output_uri + '/'
    gcs_destination = {'uri': output_uri}
    output_config = {
        'gcs_destination': gcs_destination,
        'batch_size': batch_size,
    }

    request = self._vision_service.images().asyncBatchAnnotate(
        body={'requests': img_requests, 'output_config': output_config}
    )

    logging.info('Waiting for operation to complete...')
    try:
      response = request.execute()
    except google_api_errors.HttpError as http_error:
      raise RuntimeError(
          'Error occurred calling cloud vision\n'
          f'exception: {traceback.format_exc()}'
      ) from http_error

    operation = response['name'].split('/')[-1]
    tracking_request = self._vision_service.operations().get(
        name=f'operations/{operation}'
    )
    status = tracking_request.execute()

    while (
        status['metadata']['state'].lower() != completed
        or not status[completed]
    ):
      time.sleep(time_to_sleep)
      status = tracking_request.execute()

    if status[completed] and 'response' in status:
      logging.info('Output written to GCS with prefix: %s', output_uri)
    else:
      logging.info(
          'Operation failed, check error message for more details : %s',
          status['error'],
      )

  def _write_to_csv(self) -> None:
    """Read data for all the blobs in the bucket and write to a csv."""
    url_response_map = {}
    bucket_name = self._get_bucket_name(self._output_gcs_uri)

    blobs = self._storage_client.list_blobs(bucket_name, prefix='output')
    bucket = self._storage_client.get_bucket(bucket_name)

    for blob in blobs:
      bucket_data = bucket.get_blob(blob.name)
      json_data = bucket_data.download_as_string()
      json_data = json.loads(json_data)
      for _, value in json_data.items():
        for response in value:
          url_response_map[response['context']['uri']] = response

    responses_df = pd.DataFrame(
        url_response_map.items(), columns=['Url', 'Response']
    )
    bucket.blob('csv_response/responses.csv').upload_from_string(
        responses_df.to_csv(), 'text/csv'
    )

  def run_async_batch_annotate_images(
      self, batch_size: int, time_to_sleep: int, write_to_csv: bool = False
  ) -> None:
    """Annotate images in a batch mode asynchronously.

    Args:
      batch_size: The max number of responses to output in each JSON file.
      time_to_sleep: Time to wait in seconds for checking status.
      write_to_csv: Bool argument to decide output format.
    """
    uris = self._get_all_images(self._input_gcs_uri)
    requests_collection = self._create_requests(uris, self._features)
    self._async_batch_annotate_images(
        requests_collection,
        batch_size=batch_size,
        time_to_sleep=time_to_sleep,
        output_uri=self._output_gcs_uri,
    )
    if write_to_csv:
      self._write_to_csv()
