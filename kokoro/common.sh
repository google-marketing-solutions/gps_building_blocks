#!/bin/bash

# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Install virtualenv
sudo apt-get install virtualenv -qq

# Prepare the local Python virtualenv
PYTHON3=$(which python3);
ROOT=$PWD
virtualenv --python=${PYTHON3} venv
source "${ROOT}/venv/bin/activate"

# Install necessary dependencies
python3 -m pip install --upgrade pip
python3 -m pip install -r "${ROOT}/py/requirements.txt"
pip install nose

# Execute tests
nosetests py/gps_building_blocks/cloud

if [ $? -ne 0 ]; then
  echo "Detected failed tests. Exiting with error code."
  exit $?
else
  if [ "$1" = "release" ]; then
    # Run release script
    ./kokoro/release.sh
  fi
fi
