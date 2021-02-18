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

"""An abstract interface class for output hooks.
"""

import abc

from airflow.hooks import base_hook

from gps_building_blocks.tcrm.utils import blob


class OutputHookInterface(abc.ABC, base_hook.BaseHook):
  """An abstract interface class for output hooks."""

  @abc.abstractmethod
  def send_events(self, blb: blob.Blob) -> blob.Blob:
    """Sends all events in the list to the output resource.

    The function will return a report tuple with information about the sending
    status.

    The returned report tuple will contain:
     - A list of (id, event, error_num) tuples conntaining the following info
       in the blob's failed_events fiels using Blob.append_failed_events:
         - id: start_id + the index of the event in events list.
         - event: the JSON event.
         - error: The errors.MonitoringIDsMap error ID.
     - Any number of additional Necessary information items as a list in the
       blob'r report field  using Blob.append_reports.

    Args:
      blb: A blob object containing list of the events to be sent.

    Returns:
      The input blob updated with information about the sending status.
    """
