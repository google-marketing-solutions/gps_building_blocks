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

"""A fake implementation of firestore for unit testing.
"""

import copy
from typing import Any, Dict, Optional


class FakeTransaction:
  """Simple fake transaction.

    Note this class does not actually perform constancy, it just falls back to
      simple updates.
  """

  def __init__(self, client: 'FakeFirestore'):
    # These fields does not have any effects, they are just there to ensure
    # no crashes will happen when firestore tries to operate on the transaction.
    self._client = client
    self._max_attempts = 1
    self._id = 'id'

  def _begin(self, retry_id):
    pass

  def _clean_up(self):
    pass

  def _rollback(self):
    pass

  def _commit(self):
    pass

  def update(self,
             doc_ref: 'FakeDocumentReference',
             updates: Any):
    doc_ref.update(updates)

  def set(self,
          doc_ref: 'FakeDocumentReference',
          new_value: Any):
    doc_ref.set(new_value)


class FakeDocumentSnapshot:
  """Fake document snapshot."""

  def __init__(self,
               doc_ref: 'FakeDocumentReference', doc_id: str, data: Any,
               transaction: Optional[FakeTransaction] = None):
    """Initializes document snapshot.

    Args:
      doc_ref: The document reference.
      doc_id: Document id.
      data: Document data.
      transaction: Optional transaction.
    """
    self._id = doc_id
    self._reference = doc_ref
    self._data = copy.deepcopy(data)
    self._transaction = transaction

  @property
  def id(self) -> str:
    """Returns document id."""
    return self._id

  @property
  def reference(self) -> 'FakeDocumentReference':
    """Returns document reference."""
    return self._reference

  def get(self, field_name: str) -> Any:
    """Returns field value in the data.

    Args:
      field_name: Field name
    Returns:
      Field value.
    """
    return self._data.get(field_name)

  def to_dict(self) -> Dict[str, Any]:
    """Gets the data contained in this snapshot.

    Returns:
      Data contained in this snapshot.
    """
    return self._data


class FakeDocumentReference:
  """Fake document reference."""

  def __init__(self, parent: Any,
               doc_id: str, data: Dict[str, Any]):
    """Initializes document.

    Args:
      parent: Parent.
      doc_id: Document id.
      data: Fake data for the document.
    """
    self._parent = parent
    self._data = data
    self._id = doc_id

  @property
  def id(self) -> str:
    """Returns document id."""
    return self._id

  def collection(self, collection_id: str) -> 'FakeCollectionReference':
    """Gets collection from document.

    Args:
      collection_id: Collection id.

    Returns:
      Collection with collection_id.
    """
    if collection_id not in self._data:
      self._data[collection_id] = {}
    return FakeCollectionReference(self, collection_id,
                                   self._data[collection_id])

  def set(self, new_value: Any):
    """Sets new value to document.

    Args:
      new_value: new value to be set
    """
    self._data.clear()
    self._data.update(new_value)

  def update(self, updates: Any):
    """Updates new values to the document.

    Args:
      updates: updates to apply.
    """
    self._data.update(updates)

  def get(
      self,
      transaction: Optional[FakeTransaction] = None) -> FakeDocumentSnapshot:
    """Gets a document snapshot.

    Args:
      transaction: transaction object.
    Returns:
      Document snapshot.
    """
    return FakeDocumentSnapshot(self, self._id, self._data, transaction)

  def delete(self):
    """Deletes this document."""
    self._parent.delete_child(self.id)

  def delete_child(self, child_id: str):
    """Delete child from this document.

    Args:
      child_id: Child id.
    """
    del self._data[child_id]


class FakeCollectionReference:
  """Fake collection reference."""

  def __init__(self, parent: Any, name: str, data: Any):
    """Initializes fake collection reference.

    Args:
      parent: Parent.
      name: Collection name.
      data: Fake data.
    """
    self._parent = parent
    self._name = name
    self._data = data

  def document(self, doc_id) -> FakeDocumentReference:
    """Gets document from collection.

    Args:
      doc_id: document id.
    Returns:
      document with document_id.
    """
    if doc_id not in self._data:
      self._data[doc_id] = {}
    return FakeDocumentReference(self, doc_id, self._data[doc_id])

  def stream(self):
    """Streams document from collection.

    Yields:
      document stream.
    """
    for doc_id in sorted(self._data.keys()):
      doc = FakeDocumentReference(self, doc_id, self._data[doc_id])
      yield FakeDocumentSnapshot(doc, doc_id, self._data[doc_id])

  def delete(self):
    """Deletes this collection."""
    self._parent.delete_child(self._name)

  def delete_child(self, child_id: str):
    """Deletes child from this collection.

    Args:
      child_id: Child id.
    """
    del self._data[child_id]


class FakeFirestore:
  """Fake firestore client."""

  def __init__(self):
    """Initializes firestore client."""
    self._data = {}

  def _collection(self, name: str) -> FakeCollectionReference:
    """Gets a collection object.

    Args:
      name: name of collection.
    Returns:
      collection object.
    """
    if name not in self._data:
      self._data[name] = {}
    return FakeCollectionReference(self, name, self._data[name])

  def collection(self, *path: str) -> FakeCollectionReference:
    """Gets a collection object with paths.

    Args:
      *path: Collection paths. Can be either a single name, or (col_id, doc_id,
        col_id).
    Returns:
      Collection reference.
    Raises:
      ValueError: if path length is not either 1 or 3.
    """
    length = len(path)
    if length != 1 and length != 3:
      raise ValueError(
          f'Calling collection with {len(path)} args not supported')

    if length == 1:
      return self._collection(path[0])
    else:
      return self._collection(path[0]).document(path[1]).collection(path[2])

  def transaction(self) -> FakeTransaction:
    """Returns a fake transaction.

    Returns:
      Fake transaction object.
    """
    return FakeTransaction(self)

  def delete_child(self, child_id: str):
    """Deletes child from this collection.

    Args:
      child_id: Child id.
    """
    del self._data[child_id]
