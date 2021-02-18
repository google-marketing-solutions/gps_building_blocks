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

"""An abstract interface class for input hooks.
"""

import abc
from typing import Generator, Tuple

from airflow.hooks import base_hook

from gps_building_blocks.tcrm.utils import blob


class InputHookInterface(abc.ABC, base_hook.BaseHook):
  """An abstract interface class for input hooks."""

  @abc.abstractmethod
  def events_blobs_generator(
      self,
      processed_blobs_generator: Generator[Tuple[str, str], None, None] = None
  ) -> Generator[blob.Blob, None, None]:
    """Generates all event from the input source as blobs.

    The returned generator will go over all the events in the input source based
    on the specified location given to the class at init time.

    The yielded blobs will contain:
     - An events list consisting of all the events in the blob
     - The input location
     - The location of the blob inside the input source
     - Number of events in blob.

    A blob with an empty events list will be generated if the blob was indeed
    empty. If an error ocurred during reading or parsing the blob's contents
    this blob will be skipped and not generated.
    Args:
      processed_blobs_generator: A generator that provides the processed blob
        information that helps skip read ranges.

    Yields:
      A generator that generates Blob objects from blob events contents with the
      input source.

    Raises:
      DataInConnectorError: When resource is unavailable or returns an error.
    """

  @abc.abstractmethod
  def get_location(self) -> str:
    """Retrieves the location of the input source.

    Returns:
      The location of the input source.
    """
