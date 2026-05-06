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
        "absl-py==2.0.0",
        "apache-airflow>=2.7.2,<4.0",
        "apache-airflow-providers-google>=10.10.1,<11.0",
        "google-api-core[grpc]>=2.12.0,<3.0",
        "google-api-python-client>=2.105.0,<3.0",
        "google-auth>=2.23.3,<3.0",
        "google-cloud-bigquery>=3.12.0,<4.0",
        "google-cloud-firestore>=2.13.0,<3.0",
        "google-cloud-pubsub>=2.18.4,<3.0",
        "google-cloud-storage>=2.12.0,<3.0",
        "importlib-resources>=6.1.0,<7.0",
        "lightgbm==4.1.0",
        "matplotlib==3.7.3",
        "networkx==3.1",
        "numpy>=1.24.3,<2.0",
        "pandas>=2.0.3,<3.0",
        "pandas-gbq==0.19.2",
        "parameterized==0.9.0",
        "plotly==5.18.0",
        "requests>=2.31.0,<3.0",
        "setuptools==66.1.1",
        "scikit-learn==1.1.3",
        "scipy==1.10.1",
        "seaborn==0.12.2",
        "sqlparse==0.4.4",
        "statsmodels==0.14.0",
        "tensorflow==2.13.1",
        "tensorflow-hub==0.15.0",
        "tensorflow-probability==0.21.0",
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
