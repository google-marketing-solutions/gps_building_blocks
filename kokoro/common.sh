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

# Prepare the local Python virtualenv
sudo apt-get install python3-venv
pyenv global 3.7.2

ROOT=$PWD
python -m venv venv
source venv/bin/activate

# Install necessary dependencies
python -m pip install --upgrade pip
python -m pip install -r "${ROOT}/py/requirements.txt"
python -m pip install nose

# Execute tests
nosetests py/gps_building_blocks/cloud
NOSE_EXIT_CODE=$?
if [ $NOSE_EXIT_CODE -ne 0 ]; then
  echo "Detected failed tests. Exiting with code $NOSE_EXIT_CODE."
  exit $NOSE_EXIT_CODE
else
  if [ "$1" = "release" ]; then
    # Run release script
    ./kokoro/release.sh
  fi
fi
