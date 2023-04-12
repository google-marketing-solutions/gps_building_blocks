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

"""Test for gps_building_blocks.vision_api.vision_service."""
from unittest import mock

from gps_building_blocks.vision_api import vision_service
from absl.testing import absltest


class VisionServiceTest(absltest.TestCase):

  @mock.patch.object(
      vision_service.ExternalVisionService,
      'run_async_batch_annotate_images',
      autospec=True,
  )
  def test_run_async_batch_annotate_images(
      self, mock_run_async_batch_annotate_images
  ):
    input_gcs_uri = self.create_tempdir('input').full_path
    output_gcs_uri = self.create_tempdir('output').full_path
    features = {'LABEL_DETECTION': 20, 'IMAGE_PROPERTIES': 20}
    external_service = vision_service.ExternalVisionService(
        input_gcs_uri, output_gcs_uri, features
    )

    external_service.run_async_batch_annotate_images(
        batch_size=4, time_to_sleep=5
    )

    mock_run_async_batch_annotate_images.assert_called_once()

  @mock.patch.object(
      vision_service.ExternalVisionService,
      'run_async_batch_annotate_images',
      autospec=True,
  )
  def test_invalid_directories(
      self, mock_run_async_batch_annotate_images
  ):
    input_gcs_uri = None
    output_gcs_uri = None
    features = {'LABEL_DETECTION': 20, 'IMAGE_PROPERTIES': 20}
    external_service = vision_service.ExternalVisionService(
        input_gcs_uri, output_gcs_uri, features
    )

    external_service.run_async_batch_annotate_images(
        batch_size=4, time_to_sleep=5
    )

    self.assertRaises(TypeError, mock_run_async_batch_annotate_images)

  @mock.patch.object(
      vision_service.ExternalVisionService,
      'run_async_batch_annotate_images',
      autospec=True,
  )
  def test_batch_size(
      self, mock_run_async_batch_annotate_images
  ):
    input_gcs_uri = self.create_tempdir('input').full_path
    output_gcs_uri = self.create_tempdir('output').full_path
    features = {'LABEL_DETECTION': 20, 'IMAGE_PROPERTIES': 20}
    external_service = vision_service.ExternalVisionService(
        input_gcs_uri, output_gcs_uri, features
    )
    file_count = 2
    expected_call_count = 1

    external_service.run_async_batch_annotate_images(
        batch_size=file_count, time_to_sleep=5
    )

    self.assertEqual(
        expected_call_count, mock_run_async_batch_annotate_images.call_count
    )

  @mock.patch.object(
      vision_service.ExternalVisionService,
      'run_async_batch_annotate_images',
      autospec=True,
  )
  def test_batch_size_greater_than_image_count(
      self, mock_run_async_batch_annotate_images
  ):
    input_gcs_uri = self.create_tempdir('input').full_path
    output_gcs_uri = self.create_tempdir('output').full_path
    features = {'LABEL_DETECTION': 20, 'IMAGE_PROPERTIES': 20}
    external_service = vision_service.ExternalVisionService(
        input_gcs_uri, output_gcs_uri, features
    )
    file_count = 2

    external_service.run_async_batch_annotate_images(
        batch_size=file_count + 1, time_to_sleep=5
    )

    # If batch size is greator than image count,
    # it should still pass and create 1 file of response for all the images.
    mock_run_async_batch_annotate_images.assert_called_once()

  @mock.patch.object(
      vision_service.ExternalVisionService,
      '_get_all_images',
      autospec=True,
  )
  def test_empty_directory(self, mock_get_all_images):
    input_gcs_uri = self.create_tempdir('input').full_path
    output_gcs_uri = self.create_tempdir('output').full_path
    features = {'LABEL_DETECTION': 20, 'IMAGE_PROPERTIES': 20}
    external_service = vision_service.ExternalVisionService(
        input_gcs_uri, output_gcs_uri, features
    )

    external_service._get_all_images(input_gcs_uri)

    self.assertRaises(Exception, mock_get_all_images)

if __name__ == '__main__':
  absltest.main()
