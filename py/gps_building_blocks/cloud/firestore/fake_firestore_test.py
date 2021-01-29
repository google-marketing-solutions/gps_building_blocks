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

"""Tests for gps_building_blocks.cloud.firestore.fake_firestore."""

from google.cloud import firestore

from absl.testing import absltest
from gps_building_blocks.cloud.firestore import fake_firestore


class FakeFirestoreTest(absltest.TestCase):

  def test_create_db_client(self):
    client = fake_firestore.FakeFirestore()
    self.assertIsNotNone(client)
    self.assertIsNotNone(client._data)

  def test_set_values(self):
    client = fake_firestore.FakeFirestore()
    doc_ref = client.collection('test_collection').document('test_doc')
    doc_ref.set({'foo': 'bar'})

    doc = doc_ref.get().to_dict()
    self.assertDictEqual(doc, {'foo': 'bar'},
                         'should add a new document')

    doc_ref.set({'bar': 'baz'})
    doc2 = doc_ref.get().to_dict()
    self.assertDictEqual(doc2, {'bar': 'baz'},
                         'should overwrite an existing document')

  def test_update_values(self):
    client = fake_firestore.FakeFirestore()
    doc_ref = client.collection('test_collection').document('test_doc')
    doc_ref.set({'foo': 'bar'})
    doc_ref.update({'bar': 'baz'})

    doc = doc_ref.get().to_dict()
    self.assertDictEqual(doc, {'foo': 'bar', 'bar': 'baz'},
                         'should add a new key')

    doc_ref.update({'bar': 'baz2'})
    doc2 = doc_ref.get().to_dict()
    self.assertDictEqual(doc2, {'foo': 'bar', 'bar': 'baz2'},
                         'should update an existing key')

  def test_successful_transaction(self):
    client = fake_firestore.FakeFirestore()
    doc_ref = client.collection('test_collection').document('test_doc')
    doc_ref.set({'foo': 'bar'})

    @firestore.transactional
    def trans_func(transaction):
      snapshot = doc_ref.get(transaction=transaction)
      if snapshot.get('foo') == 'bar':
        transaction.update(doc_ref, {'bar': 'baz'})

    trans_func(client.transaction())

    doc = doc_ref.get().to_dict()
    self.assertDictEqual(doc, {'foo': 'bar', 'bar': 'baz'},
                         'transaction should succeed')

  def test_no_update_in_transaction(self):
    client = fake_firestore.FakeFirestore()
    doc_ref = client.collection('test_collection').document('test_doc')
    doc_ref.set({'foo': 'bar'})

    @firestore.transactional
    def trans_func(transaction):
      snapshot = doc_ref.get(transaction=transaction)
      if snapshot.get('foo') == 'baz':
        transaction.update(doc_ref, {'bar': 'baz'})

    trans_func(client.transaction())

    doc = doc_ref.get().to_dict()
    self.assertDictEqual(doc, {'foo': 'bar'},
                         'transaction should not update data')

  def test_delete_document(self):
    client = fake_firestore.FakeFirestore()
    col_ref = client.collection('test_collection')
    doc_ref = col_ref.document('test_doc')
    doc_ref.set({'foo': 'bar'})

    self.assertIn('test_doc', col_ref._data)
    doc_ref.delete()
    self.assertNotIn('test_doc', col_ref._data)

  def test_delete_collection(self):
    client = fake_firestore.FakeFirestore()
    col_ref = client.collection('test_collection')

    self.assertIn('test_collection', client._data)
    col_ref.delete()
    self.assertNotIn('test_collection', client._data)


if __name__ == '__main__':
  absltest.main()
