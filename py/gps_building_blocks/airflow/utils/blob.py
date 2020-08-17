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


"""A Blob class for data-in representation.

The Blob class contains all JSON events and all necessary metadata to the
operators.
"""
import enum

from typing import List, Text, Dict, Any


class BlobStatus(enum.Enum):
  UNPROCESSED = enum.auto()
  PROCESSED = enum.auto()
  ERROR = enum.auto()


class Blob(object):
  """A Blob class for data-in representation.

  The Blob class contains all JSON events and all necessary metadata to the
  operators.

  Attributes:
    events: A list of JSON events to be sent.
    blob_id: Blob unique ID. Usually composed of a GCP URL, optionally appended
    with the blob's location for distinction.
    platform: The platform which the data comes from.
    source: The source of the events.
    location: The specific object location of the events within the source.
    position: The events starting position within the object.
    status: One of the blob statuses in BlobStatus.
    status_desc: A description of the current blob status.
    unsent_events_indexes: List of unsuccesfully sent events indexes.
    num_events: Number of events in blob. Defaults to length of events list.
  """

  def __init__(self, events: List[Dict[Text, Any]], blob_id: Text,
               platform: Text, source: Text, location: Text, position: int = 0,
               status: BlobStatus = BlobStatus.UNPROCESSED,
               status_desc: Text = '', unsent_events_indexes: List[int] = None,
               num_events: int = None) -> None:
    """Initiates Blob with events and location metadata."""
    self.events = events
    self.blob_id = blob_id
    self.platform = platform
    self.source = source
    self.location = location
    self.position = position
    self.status = status
    self.status_desc = status_desc
    self.num_events = num_events if num_events is not None else len(events)
    self.unsent_events_indexes = (
        list() if not unsent_events_indexes else unsent_events_indexes)
