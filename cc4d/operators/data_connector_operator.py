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

"""Data Connector Operator to send data from input source to output source."""

from typing import Any, List, Mapping, Optional, Text, Tuple, Union
from airflow import models

from gps_building_blocks.cc4d.hooks import ds_hook
from gps_building_blocks.cc4d.hooks import input_hook_interface
from gps_building_blocks.cc4d.hooks import output_hook_interface
from gps_building_blocks.cc4d.utils import blob
from gps_building_blocks.cc4d.utils import errors


class DataConnectorOperator(models.BaseOperator):
  """Custom Operator to send data from an input hook to an output hook."""

  def __init__(self, input_hook: input_hook_interface.InputHookInterface,
               output_hook: output_hook_interface.OutputHookInterface, *args,
               return_report: bool = False, enable_datastore: bool = True,
               **kwargs) -> None:
    super(DataConnectorOperator, self).__init__(*args, **kwargs)
    self.input_hook = input_hook
    self.output_hook = output_hook
    self.return_report = return_report
    self.enable_datastore = enable_datastore

  def execute(self, context: Mapping[Text, Any]
             ) -> Optional[List[Tuple[Union[List[Any]]]]]:
    """Executes this Operator.

    Retrieves all blobs with from input_hook and sends them to output_hook.
    Updates datastore with each blob's status upon success or failure.

    TODO(saraid): Returned reports about sending blob info is now sent as an
    ordered list, making it hard to identify which blob the report belongs to.
    Thus for blobs with no events an empty tuple is appended to the reports
    list. Find a better format for returning blob related reports.

    Args:
      context: Unused.

    Returns:
      A list of tuples of any data returned from output_hook if return_report
      flag is set to True.
    """
    if self.enable_datastore:
      datastore_hook = ds_hook.DatastoreHook()

    reports = []
    for blb in self.input_hook.events_blobs_generator():
      if blb.events:
        report = self.output_hook.send_events(blb.events)
        reports.append(report)
        # Based on the OutputHookInterface send_events returns a report tuple
        # for each blob. If the report contains any unsent event indexes the
        # blob's status is changed to ERROR with the appropriate error status
        # description for monitoring.
        if report[1]:
          blb.unsent_events_indexes = report[1]
          blb.status = blob.BlobStatus.ERROR
          blb.status_desc = 'Error: There are some unsent events in this blob'
        else:
          blb.status = blob.BlobStatus.PROCESSED
      else:
        # Based on the InputHookInterface the events_blobs_generator will
        # generate blobs with empty events list if the blob was indeed empty or
        # in case an error occurred while reading the blob. If an error occurred
        # the blob will also have an ERROR status and an appropriate status
        # description. Otherwise the blob's status will be changed to PROCESSED.
        reports.append(())
        if blb.status is not blob.BlobStatus.ERROR:
          blb.status = blob.BlobStatus.PROCESSED

      if self.enable_datastore:
        try:
          datastore_hook.insert_or_update_blob_information(blb)
        except errors.DatastoreError as error:
          self.log.error(str(error))

    if self.return_report:
      return reports
