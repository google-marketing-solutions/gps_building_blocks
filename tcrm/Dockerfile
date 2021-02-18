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
#
# Docker file for running TCRM cloudbuid CI tests.

FROM python:3.7-slim-buster

COPY requirements.txt /requirements.txt

RUN set -ex \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        build-essential \
    && pip install -r /requirements.txt \
    && pip install --upgrade protobuf \
    && pip install mock \
        pytest \
        requests_mock \
        freezegun \
        pytest-cov

ADD . /root/tcrm

RUN mkdir -p /root/tcrm/src/gps_building_blocks/cloud/utils
RUN cp -r /root/tcrm/gps_building_blocks/py/gps_building_blocks/cloud/utils/* /root/tcrm/src/gps_building_blocks/cloud/utils

ENV PYTHONPATH=".:./src"

WORKDIR /root/tcrm/src
