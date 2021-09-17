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

import os

import setuptools

_README = "README.md"
_EXT_README = "EXTERNAL_" + _README
_VERSION = "VERSION"
path = _EXT_README if os.path.isfile(_EXT_README) else _README

with open(path, "r") as fh:
  long_description = fh.read()

with open(_VERSION, "r") as version_file:
  version = version_file.read().strip()

setuptools.setup(
    name="gps-building-blocks",
    version=version,
    author="gPS Team",
    author_email="no-reply@google.com",
    description="Modules and tools useful for use with advanced data solutions on Google Ads, Google Marketing Platform and Google Cloud.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/google/gps_building_blocks",
    license="Apache Software License",
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=[
        "absl-py==0.10.0",
        "apache-airflow>=1.10.14,<2.0",
        "decorator==4.4.0",
        "google-api-core==1.21.0",
        "google-api-python-client==1.9.1",
        "google-auth==1.18.0",
        "google-cloud-bigquery==1.22.0",
        "google-cloud-firestore==1.6.2",
        "google-cloud-storage==1.28.1",
        "google-cloud-pubsub==1.3.1",
        "importlib-resources==1.5.0",
        "matplotlib==3.3.3",
        "networkx==2.5.0",
        "numpy==1.19.5",
        "pandas==1.0.5",
        "pandas_gbq==0.14.1",
        "parameterized==0.8.1",
        "plotly==4.14.3",
        "requests==2.23.0",
        "scipy==1.2.1",
        "six==1.15.0",
        "sklearn==0.0.0",
        "tensorflow==2.4.1",
        "tensorflow-hub==0.11.0",
        "statsmodels==0.12.1",
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
