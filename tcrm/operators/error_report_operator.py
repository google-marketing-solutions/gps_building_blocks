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
"""Custom operator for reporting configuration errors."""
import logging
from typing import Any, Dict

from airflow import models


class ErrorReportOperator(models.BaseOperator):
  """Custom operator for reporting configuration errors."""

  def __init__(self, task_id: str, error: Exception, dag: models.DAG = None
               ) -> None:
    """Initiates the ErrorReportOperator.

    Args:
      task_id: Id of the task.
      error: Any exception that needs to report.
      dag: The dag that the task attaches to.
    """

    super().__init__(task_id=task_id, retries=0, dag=dag)

    self.error = error

  def execute(self, context: Dict[str, Any]) -> None:
    """Executes this Operator.

    Args:
      context: Unused.

    Raises:
      UserWarning: Raises for terminating the task.
    """
    error_message = '\n' + '*' * 128 + '\n'
    error_message += str(self.error) + '\n'
    error_message += '*' * 128
    logging.warning(error_message)
    raise ValueError()
