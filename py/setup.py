# Lint as: python3
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

"""Config file for distributing package via Pypi server."""

import setuptools


# It is assumed that this file will moved to gps_building_block/py/setup.py,
# while the README resides at gps_building_blocks/README.md.

with open("../README.md", "r") as fh:
  long_description = fh.read()

setuptools.setup(
    name="gps-building-blocks",
    version="0.1.12",
    author="gPS Team",
    author_email="no-reply@google.com",
    description="Modules and tools useful for use with advanced data solutions on Google Ads, Google Marketing Platform and Google Cloud.",
    long_description=long_description,
    long_description_tpye="text/markdown",
    url="https://github.com/google/gps_building_blocks",
    license="Apache Software License",
    packages=setuptools.find_packages(),
    install_requires=[
        "absl-py==0.9.0",
        "google-api-core==1.17.0",
        "google-api-python-client==1.9.1",
        "google-auth==1.16.0",
        "google-cloud-storage==1.28.1",
        "requests==2.23.0",
        "dataclasses; python_version<'3.7'"
    ],
    classifiers=[
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Internet",
        "Topic :: Scientific/Engineering",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Operating System :: OS Independent",
    ],
)
