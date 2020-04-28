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
from typing import Any, Dict, List, Tuple, Union

from airflow.hooks import base_hook


class OutputHookInterface(abc.ABC, base_hook.BaseHook):
  """An abstract interface class for output hooks."""

  @abc.abstractmethod
  def send_events(self, events: List[Dict[Any, Any]]
                 ) -> Tuple[Union[List[Any]]]:
    """Sends all events in the list to the output resource.

    The function will return a report tuple with information about the sending
    status.

    The returned report tuple will contain:
     - A list of all successfully sent event indexes
     - A list of all successfully sent event indexes
     - Any number of additional Necessary information items

    Args:
      events: A list of the events to be sent.

    Returns:
      A tuple containing a report about the sending status.
    """
    pass
