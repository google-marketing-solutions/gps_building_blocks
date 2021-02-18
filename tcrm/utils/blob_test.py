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

"""Tests for tcrm.utils.blob."""

import unittest

from gps_building_blocks.tcrm.utils import blob


class BlobTest(unittest.TestCase):

  def test_init(self):
    blb = blob.Blob([{'': ''}], 'Location', 0)

    self.assertTupleEqual((blb.events,
                           blb.location,
                           blb.position,
                           blb.num_rows,
                           blb.failed_events,
                           blb.reports),
                          ([{'': ''}], 'Location', 0, 1, [], []))

  def test_append_failed_events(self):
    blb = blob.Blob([{'': ''}], 'Location', 0)

    blb.append_failed_events([(1, {'a': 1}, 12)])

    self.assertListEqual(blb.failed_events, [(1, {'a': 1}, 12)])

  def test_append_failed_events_empty_list(self):
    blb = blob.Blob([{'': ''}], 'Location', 0)

    blb.append_failed_events([])

    self.assertListEqual(blb.failed_events, [])

  def test_append_failed_event(self):
    blb = blob.Blob([{'': ''}], 'Location', 0)

    blb.append_failed_event(index=1, event={'a': 1}, error_num=12)

    self.assertListEqual(blb.failed_events, [(1, {'a': 1}, 12)])

  def test_extend_reports(self):
    blb = blob.Blob([{'': ''}], 'Location', 0)

    blb.extend_reports([(1, '', 12)])

    self.assertListEqual(blb.reports, [(1, '', 12)])

  def test_extend_reports_empty_list(self):
    blb = blob.Blob([{'': ''}], 'Location', 0)

    blb.extend_reports([])

    self.assertListEqual(blb.reports, [])


if __name__ == '__main__':
  unittest.main()
