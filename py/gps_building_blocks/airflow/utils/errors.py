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

"""Errors file for this data connector component.

All exceptions defined by the library should be in this file.
"""

from typing import Optional, Text


class Error(Exception):
  """Base error class for all Exceptions.

  Can store a custom message and a previous error, if exists, for more
  details and stack tracing use.
  """

  def __init__(self, msg: Text = '', error: Optional[Exception] = None) -> None:
    super(Error, self).__init__()
    self.msg = msg
    self.prev_error = error

  def __repr__(self) -> Text:
    reason = '%s' % type(self).__name__
    if self.msg:
      reason += ' %s' % self.msg
    if self.prev_error:
      reason += ': %s' % str(self.prev_error)
    return reason

  __str__ = __repr__


# Datastore related errors
class DatastoreError(Error):
  """Raised when Datastore returns an error."""
  pass


class DatastoreRunQueryError(DatastoreError):
  """Raised when querying Datastore returns an error."""
  pass


class DatastoreCommitError(DatastoreError):
  """Error occurred while committing to Datastore."""
  pass


class DatastoreInsertBlobInfoError(DatastoreCommitError):
  """Error occurred while inserting blob info into Datastore."""
  pass


class DatastoreUpdateBlobInfoError(DatastoreCommitError):
  """Error occurred while updating blob info in Datastore."""
  pass


# Data in connector related errors
class DataInConnectorError(Error):
  """Raised when an input data source connector returns an error."""
  pass


class DataInConnectorBlobParseError(DataInConnectorError):
  """Error occurred while parsing blob contents."""
  pass


class DataInConnectorValueError(DataInConnectorError):
  """Error occurred due to a wrong value being passed on."""
  pass


# Data out connector related errors
class DataOutConnectorError(Error):
  """Raised when an output data source connector returns an error."""
  pass


class DataOutConnectorValueError(DataOutConnectorError):
  """Error occurred due to a wrong value being passed on."""
  pass


class DataOutConnectorInvalidPayloadError(DataOutConnectorError):
  """Error occurred constructing or handling payload."""
  pass


class DataOutConnectorSendUnsuccessfulError(DataOutConnectorError):
  """Error occurred while sending data to data out source."""
  pass


class DataOutConnectorBlobReplacedError(DataOutConnectorError):
  """Error occurred while sending blob contents and Blob was replaced."""
  pass


class DataOutConnectorBlobProcessError(DataOutConnectorError):
  """Error occurred while sending some parts of blob contents."""
  pass
