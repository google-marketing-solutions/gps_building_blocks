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

#!/bin/bash
# TCRM Cloud Environment setup script.

set -e

VIRTUALENV_PATH=$HOME/"tcrm-venv"

# Create virtual environment with python3
if [[ ! -d "${VIRTUALENV_PATH}" ]]; then
  virtualenv -p python3 "${VIRTUALENV_PATH}"
fi

# Activate virtual environment.
source "$VIRTUALENV_PATH"/bin/activate

# Install Python dependencies.
pip install -r requirements.txt

# Download TCRM Dependencies.
git clone "https://cse.googlesource.com/common/gps_building_blocks"
mkdir -p src/gps_building_blocks/cloud/utils
cp -r gps_building_blocks/py/gps_building_blocks/cloud/utils/* src/gps_building_blocks/cloud/utils

# Setup cloud environment.
PYTHONPATH=src:$PYTHONPATH
export PYTHONPATH
python cloud_env_setup.py "$@"
