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
from typing import Generator

from airflow.hooks import base_hook

from gps_building_blocks.airflow.utils import blob


class InputHookInterface(abc.ABC, base_hook.BaseHook):
  """An abstract interface class for input hooks."""

  @abc.abstractmethod
  def events_blobs_generator(self) -> Generator[blob.Blob, None, None]:
    """Generates all event blobs from the input source.

    The returned generator will go over all the blobs in the input source based
    on the specified location given to the class at init time.

    The yielded blobs will contain:
     - An events list consisting of all the events in the blob
     - The input source
     - The location of the blob inside the input source
     - The status, defaults to UNPROCESSED (if the blob was parsed successfully)
     - A message, defaults to an empty string

    A blob with an empty events list will be generated if the blob was indeed
    empty or if an error ocurred during reading or parsing the blob's contents.
    Should an error occur during blob parsing, the blob's status field will be
    set to ERROR and an error message will be stored in the `msg` field.

    Yields:
      A generator that generates Blob objects from blob events contents with the
      input source.

    Raises:
      DataInConnectorError: When resource is unavailable or returns an error.
    """
    pass
