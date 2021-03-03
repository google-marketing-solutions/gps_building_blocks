# Copyright 2021 Google LLC.
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
#
# Returns the given fact_name and fact_value encoded as a legal and unique BigQuery column name.
# Format: <fact_name>_<encoded_fact_value>_<hashed_fact_value> where:
#   <fact_name>: Already a legal BigQuery column name.
#   <encoded_fact_value>: Any characters in the value that are not legal in a BigQuery column name
#                         are replaced by _. Also, if necessary, the tail of the value is docked to
#                         fit inside the 100 character limit of AutoML.
#   <hashed_fact_value>: FARM_FINGERPRINT of the fact_value to distinguish between two distinct
#                        values that have the same encoded_fact_value, e.g "value/" and "value\"
# Note that the function will die if the length of the fact_name and hashed_fact_value exceeds 74.
# In this case, the fact name should be renamed.
CREATE TEMP FUNCTION ColumnName(fact_name STRING, fact_value STRING) AS (
  CASE
    WHEN fact_value IS NULL THEN CONCAT(fact_name, '_NULL')
  ELSE
    REGEXP_REPLACE(
      CONCAT(fact_name,
             '_',
             # Dock the tail of the fact value so that the column name fits within 74 characters,
             # leaving 26 characters for the feature name and separating underscores. This helps
             # ensure that the final column name fits within the AutoML 100 character limit.
             LEFT(fact_value, GREATEST(
               0, 74 - LENGTH(fact_name) - LENGTH(CAST(FARM_FINGERPRINT(fact_value) AS STRING)))),
             '_',
             FARM_FINGERPRINT(fact_value)
      ), '[^a-zA-Z0-9_]', '_')
  END
);
