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

"""Tests for airflow.utils.blob."""

from absl.testing import absltest

from gps_building_blocks.airflow.utils import blob


class BlobTest(absltest.TestCase):

  def test_init(self):
    blob_instance = blob.Blob([{'': ''}], 'id', 'GCP', 'Source', 'Location', 0)

    self.assertTupleEqual((blob_instance.events,
                           blob_instance.blob_id,
                           blob_instance.platform,
                           blob_instance.source,
                           blob_instance.location,
                           blob_instance.position,
                           blob_instance.status,
                           blob_instance.status_desc,
                           blob_instance.unsent_events_indexes),
                          ([{'': ''}], 'id', 'GCP', 'Source', 'Location', 0,
                           blob.BlobStatus.UNPROCESSED, '', []))


if __name__ == '__main__':
  absltest.main()
