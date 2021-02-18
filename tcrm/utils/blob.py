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

# python3

"""A Blob class for data-in representation.

The Blob class contains all JSON events and all necessary metadata to the
operators.
"""

from typing import List, Dict, Any, Tuple


class Blob(object):
  """A Blob class for data-in representation.

  The Blob class contains all JSON events and all necessary metadata to the
  operators.

  Attributes:
    events: A list of JSON events to be sent.
    location: The specific object location of the events within the source.
    position: The events starting position within the object.
    failed_events: A list of (id, event, error_num) tuples conntaining the
        following info:
         - id: start_id + the index of the event in events list.
         - event: the JSON event.
         - error: The errors.MonitoringIDsMap error ID.
    num_rows: Number of events in blob. Defaults to length of events list.
    reports: any additional optional information about the blob.
  """

  def __init__(self, events: List[Dict[str, Any]],
               location: str, reports: List[Any] = None,
               failed_events: List[Tuple[int, Dict[str, Any], int]] = None,
               position: int = 0, num_rows: int = None) -> None:
    """Initiates Blob with events and location metadata."""
    self.events = events
    self.location = location
    self.position = position
    self.num_rows = num_rows if num_rows is not None else len(events)
    self.failed_events = failed_events if failed_events else list()
    self.reports = reports if reports else list()

  def append_failed_events(
      self, failed_events: List[Tuple[int, Dict[str, Any], int]]) -> None:
    """Appends the given events list to the blob's reports list."""
    self.failed_events.extend(failed_events)

  def append_failed_event(self, index: int, event: Dict[str, Any],
                          error_num: int) -> None:
    """Appends the given event to the blob's reports list."""
    self.failed_events.append((index, event, error_num))

  def extend_reports(self, report: Any) -> None:
    """Appends the given report to the blob's reports list."""
    self.reports.extend(report)
