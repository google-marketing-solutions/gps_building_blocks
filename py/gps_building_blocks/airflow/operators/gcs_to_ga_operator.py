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

"""Custom Operator to send data from Cloud Storage to Google Analytics."""

from typing import Any, Mapping, Optional, Text

from gps_building_blocks.airflow.hooks import ga_hook
from gps_building_blocks.airflow.hooks import gcs_hook
from gps_building_blocks.airflow.operators import data_connector_operator


class GoogleCloudStorageToGoogleAnalyticsOperator(
    data_connector_operator.DataConnectorOperator):
  """Custom Operator to send data from Cloud Storage to GoogleAnalytics."""

  def __init__(self,
               gcs_bucket: Text,
               gcs_prefix: Text,
               gcs_content_type: Text,
               ga_tracking_id: Text,
               ga_base_params: Optional[Mapping[Text, Any]] = None,
               ga_dry_run: Optional[bool] = False,
               **kwargs) -> None:
    """Initializes the DataConnectorOperator with an input and output hooks.

    Args:
      gcs_bucket: Unique name of the bucket holding the target blob.
      gcs_prefix: The path to a location within the bucket.
      gcs_content_type: Blob's content type. Either 'JSON' or 'CSV'.
      ga_tracking_id: Google Analytics' tracking id to identify a property.
      ga_base_params: Default parameters that serve as the base on which to
        build the Measurement Protocol payload.
      ga_dry_run: If True, this will not send real hits to the endpoint.
      **kwargs: Other arguments to pass through to the operator or hooks.
    """
    self.gcs_hook = gcs_hook.GoogleCloudStorageHook(
        bucket=gcs_bucket, prefix=gcs_prefix, content_type=gcs_content_type)
    self.ga_hook = ga_hook.GoogleAnalyticsHook(
        tracking_id=ga_tracking_id, base_params=ga_base_params,
        dry_run=ga_dry_run)
    super(GoogleCloudStorageToGoogleAnalyticsOperator, self).__init__(
        input_hook=self.gcs_hook, output_hook=self.ga_hook, **kwargs)
