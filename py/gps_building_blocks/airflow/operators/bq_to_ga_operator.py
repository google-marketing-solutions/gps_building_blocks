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

"""Custom Operator to send data from BigQuery to Google Analytics."""

from typing import Any, Mapping, Optional, Text

from gps_building_blocks.airflow.hooks import bq_hook
from gps_building_blocks.airflow.hooks import ga_hook
from gps_building_blocks.airflow.operators import data_connector_operator


class BigQueryToGoogleAnalyticsOperator(
    data_connector_operator.DataConnectorOperator):
  """Custom Operator to send data from BigQuery to Google Analytics."""

  def __init__(self,
               bq_conn_id: Text,
               bq_dataset_id: Text,
               bq_table_id: Text,
               ga_tracking_id: Text,
               bq_selected_fields: Optional[Text] = None,
               ga_base_params: Optional[Mapping[Text, Any]] = None,
               ga_dry_run: Optional[bool] = False,
               **kwargs) -> None:
    """Initializes the DataConnectorOperator with an input and output hooks.

    Args:
      bq_conn_id: Connection id passed to airflow's BigQueryHook.
      bq_dataset_id: Dataset id of the target table.
      bq_table_id: Table name of the target table.
      ga_tracking_id: Google Analytics' tracking id to identify a property.
      bq_selected_fields: Subset of fields to return (e.g. 'field_1,field_2').
      ga_base_params: Default parameters that serve as the base on which to
        build the Measurement Protocol payload.
      ga_dry_run: If True, this will not send real hits to the endpoint.
      **kwargs: Other arguments to pass through to the operator or hooks.
    """
    self.bq_hook = bq_hook.BigQueryHook(conn_id=bq_conn_id,
                                        dataset_id=bq_dataset_id,
                                        table_id=bq_table_id,
                                        selected_fields=bq_selected_fields)
    self.ga_hook = ga_hook.GoogleAnalyticsHook(tracking_id=ga_tracking_id,
                                               base_params=ga_base_params,
                                               dry_run=ga_dry_run)
    super(BigQueryToGoogleAnalyticsOperator, self).__init__(
        input_hook=self.bq_hook, output_hook=self.ga_hook, **kwargs)
