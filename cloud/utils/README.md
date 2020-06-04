# Cloud Utils

**Disclaimer: This is not an official Google product.**

Cloud Utils is a library designed for Google Cloud Platform (GCP) based
solution developers. It offers utility modules to automate common tasks in GCP.

## Table of Contents

-   [Key Features](#key-features)
-   [Modules overview](#modules-overview)
-   [How to use?](#how-to-use)
    -   [1. Cloud Auth](#1-cloud-auth)
    -   [2. Cloud API](#2-cloud-api)
    -   [3. Cloud Composer](#3-cloud-composer)
    -   [4. Cloud Storage](#4-cloud-storage)

## Key Features

1.  Simple and flexible methods to perform most common GCP tasks.
2.  Built in error handling and retry logic.
3.  Hides complexity of handling long running processes.

## Modules overview

The below diagram shows different modules and their functionalities.

<img src="images/module_overview.png" width="40%">

## How to use?

### 1. Cloud Auth

The Cloud Auth module provides useful functions to handle identity and access
management for GCP projects.

#### 1.1. Create service account

When creating a service account, the role name needs to be specified. The
following are most commonly used primitive roles.

1.  viewer
2.  editor
3.  owner

[This document](https://cloud.google.com/iam/docs/understanding-roles) describes
all the available roles. Please note that the role names should **not** contain
**"roles/"** prefix.

```python
from gps_building_blocks.cloud.utils import cloud_auth


PROJECT_ID = 'test-project'
SERVICE_ACCOUNT_NAME = 'my-svc-act-1'
ROLE_NAME = 'editor'
SERVICE_ACCOUNT_KEY_FILE = '/tmp/service_account_key.json'

cloud_auth.create_service_account(PROJECT_ID, SERVICE_ACCOUNT_NAME, ROLE_NAME,
                                  SERVICE_ACCOUNT_KEY_FILE)
```

#### 1.2. Change service account role

The following are most commonly used primitive roles.

1.  viewer
2.  editor
3.  owner

[This document](https://cloud.google.com/iam/docs/understanding-roles) describes
all the available roles. Please note that the role names should **not** contain
**"roles/"** prefix.

```python
from gps_building_blocks.cloud.utils import cloud_auth


PROJECT_ID = 'test-project'
SERVICE_ACCOUNT_NAME = 'my-svc-act-1'
ROLE_NAME = 'editor'

cloud_auth.set_service_account_role(PROJECT_ID, SERVICE_ACCOUNT_NAME, ROLE_NAME)
```

#### 1.3. Construct client to interact with an API

There are two ways to construct a client.

##### a. Using service account impersonation (Recommended)

```python
from gps_building_blocks.cloud.utils import cloud_auth

# Each Google Cloud API has different service name and versions. For composer
# the details can be found here - https://cloud.google.com/composer/docs/reference/rest.
# Use reference docs to get the service name and versions of required API.
SERVICE_NAME = 'composer'
VERSION = 'v1beta1'
SERVICE_ACCOUNT_NAME = 'my-svc-account@project-id.iam.gserviceaccount.com'

client = cloud_auth.build_impersonated_client(
    SERVICE_NAME, SERVICE_ACCOUNT_NAME, VERSION)
```

##### b. Using service account credentials

```python
from gps_building_blocks.cloud.utils import cloud_auth


SERVICE_NAME = 'composer'
SERVICE_ACCOUNT_KEY_FILE = '/tmp/service_account_key.json'

client = cloud_auth.build_service_client(SERVICE_NAME, SERVICE_ACCOUNT_KEY_FILE)
```

##### c. Using default credentials

```python
from gps_building_blocks.cloud.utils import cloud_auth


SERVICE_NAME = 'composer'

client = cloud_auth.build_service_client(SERVICE_NAME)
```

### 2. Cloud API

The Cloud API module provides utility functions for Cloud Services.

#### 2.1. Enable Cloud APIs

Multiple Cloud APIs can be enabled on the GCP project in a single function call
using Cloud API module. [Here](https://cloud.google.com/apis/docs/overview) is
the list of available Cloud APIs.

```python
from gps_building_blocks.cloud.utils import cloud_api


SERVICE_ACCOUNT_KEY_FILE = '/tmp/service_account_key.json'
APIS_TO_BE_ENABLED = ['storage-component.googleapis.com',
                      'composer.googleapis.com', 'ml.googleapis.com']

cloud_api_utils = cloud_api.CloudApiUtils(
      project_id=PROJECT_ID, service_account_key_file=SERVICE_ACCOUNT_KEY_FILE)
cloud_api_utils.enable_apis(APIS_TO_BE_ENABLED)
```

#### 2.2. Disable Cloud API

Cloud API can be disabled for the GCP project using Cloud API module.
[Here](https://cloud.google.com/apis/docs/overview) is the list of available
Cloud APIs.

```python
from gps_building_blocks.cloud.utils import cloud_auth
from gps_building_blocks.cloud.utils import cloud_api


SERVICE_ACCOUNT_KEY_FILE = '/tmp/service_account_key.json'
API_TO_BE_DISABLED = 'storage-component.googleapis.com'

session = cloud_auth.get_auth_session(SERVICE_ACCOUNT_KEY_FILE)
cloud_api.disable_api(session, PROJECT_ID, API_TO_BE_DISABLED)
```

#### 2.3. Check if Cloud API is enabled

Its possible to verify if a Cloud API has already been enabled on the GCP
project as shown below.

```python
from gps_building_blocks.cloud.utils import cloud_auth
from gps_building_blocks.cloud.utils import cloud_api


SERVICE_ACCOUNT_KEY_FILE = '/tmp/service_account_key.json'
API_TO_CHECK = 'storage-component.googleapis.com'

session = cloud_auth.get_auth_session(SERVICE_ACCOUNT_KEY_FILE)
cloud_api.is_api_enabled(session, PROJECT_ID, API_TO_CHECK)
```

### 3. Cloud Composer

The Cloud Composer module provides utility functions to manage Cloud Composer
environment.

#### 3.1. Create Cloud Composer environment

When creating Composer environment, the following parameters can be configured.

*   Name of the Composer environment
*   Location under which the Composer environment needs to be managed. Default
    value - 'us-central1'
*   [Zone](https://cloud.google.com/compute/docs/regions-zones) where the
    Composer environment will be created. Default value - 'b'
*   Disk size(GB) for Composer environment. Default value - '20 GB'
*   [Machine type](https://cloud.google.com/compute/docs/machine-types) for the
    VM. Default value - 'n1-standard-1'

```python
from gps_building_blocks.cloud.utils import cloud_composer


PROJECT_ID = 'project-id'
SERVICE_ACCOUNT_KEY_FILE = '/tmp/service_account_key.json'
ENVIRONMENT_NAME = 'environment-name'
LOCATION = 'us-west1'

cloud_composer_utils = cloud_composer.CloudComposerUtils(
    project_id=PROJECT_ID, location=LOCATION,
    service_account_key_file=SERVICE_ACCOUNT_KEY_FILE)
cloud_composer_utils.create_environment(ENVIRONMENT_NAME)
```

#### 3.2. Install python packages in Cloud Composer environment

The newly created Composer environment will only have python packages. If the
Composer environment requires a specific python package, it can be installed
using the Cloud Composer module.

Specify the package name and version specifiers as shown below: **{'tensorflow':
'<=1.0.1', 'apache-beam', '==2.12.0', 'glob2': ' '}**

Please note, for a package without the version specifier, use an empty string
for the value, such as {'glob2': ' '}

More details on installing python packages on Composer environment can be found
[here](https://cloud.google.com/composer/docs/how-to/using/installing-python-dependencies).

```python
from gps_building_blocks.cloud.utils import cloud_composer


PROJECT_ID = 'project-id'
SERVICE_ACCOUNT_KEY_FILE = '/tmp/service_account_key.json'
ENVIRONMENT_NAME = 'environment-name'
PYTHON_PACKAGES = {'tensorflow' : "<=1.0.1", 'apache-beam': '==2.12.0'}

cloud_composer_utils = cloud_composer.CloudComposerUtils(
    project_id=PROJECT_ID, service_account_key_file=SERVICE_ACCOUNT_KEY_FILE)
cloud_composer_utils.install_python_packages(ENVIRONMENT_NAME, PYTHON_PACKAGES)
```

#### 3.3. Set environment variables in Cloud Composer environment

Cloud Composer provides environment variables to the Apache Airflow scheduler,
worker, and web server processes. For example, API_KEY could be configured as
environment variable to invoke a REST API in Cloud Composer workflows. These
environment variables can be completely replaced using Cloud Composer module.

Please note that the environment variables are different from
[Airflow Variables](https://airflow.readthedocs.io/en/stable/concepts.html#variables).
While creating DAGs, the environment variables can be retrieved using
`os.environ.get` method. However, the Airflow variables should be retrieved
using `models.Variable.get` method.

More details on setting environment variables in Composer environment can be
found
[here](https://cloud.google.com/composer/docs/how-to/managing/environment-variables).

```python
from gps_building_blocks.cloud.utils import cloud_composer


PROJECT_ID = 'project-id'
SERVICE_ACCOUNT_KEY_FILE = '/tmp/service_account_key.json'
ENVIRONMENT_NAME = 'environment-name'
ENVIRONMENT_VARIABLES = {'key1' : "value1", 'key2': 'value2'}

cloud_composer_utils = cloud_composer.CloudComposerUtils(
    project_id=PROJECT_ID, service_account_key_file=SERVICE_ACCOUNT_KEY_FILE)
cloud_composer_utils.set_environment_variables(ENVIRONMENT_NAME,
                                               ENVIRONMENT_VARIABLES)
```

#### 3.4. Override Airflow configurations in Cloud Composer environment

Apache Airflow configurations can be overridden in the Cloud Composer
environment. An object containing a list of **'key': 'value'** pairs should be
given to override configurations. Property keys contain the section and property
names, separated by a hyphen as shown below:
**{'smtp-smtp_mail_from':'no-reply@abc.com', 'core-dags_are_paused_at_creation':
'True'}**

There are some configurations which are not allowed to be
[overridden](https://cloud.google.com/composer/docs/concepts/airflow-configurations#airflow_configuration_blacklists)

```python
from gps_building_blocks.cloud.utils import cloud_composer


PROJECT_ID = 'project-id'
SERVICE_ACCOUNT_KEY_FILE = '/tmp/service_account_key.json'
ENVIRONMENT_NAME = 'environment-name'
AIRFLOW_CONFIG_OVERRIDES = {
    'smtp-smtp_mail_from': 'no-reply@abc.com',
    'core-dags_are_paused_at_creation': 'True'
}

cloud_composer_utils = cloud_composer.CloudComposerUtils(
    project_id=PROJECT_ID, service_account_key_file=SERVICE_ACCOUNT_KEY_FILE)
cloud_composer_utils.override_airflow_configs(ENVIRONMENT_NAME,
                                              AIRFLOW_CONFIG_OVERRIDES)
```

### 4. Cloud Storage

The Cloud Storage module provides utility functions to manage files/blobs in
Cloud Storage.

#### 4.1. Upload files

Files from local file system can be uploaded to Cloud Storage using one of the
following ways.

##### a. Upload files by specifying Cloud Storage URL

If you have fully formed Cloud Storage URL for the destination in the form of
`gs://bucket_name/path/to/file`, use this method to upload file. The bucket will
be created if it doesn't exist.

```python
from gps_building_blocks.cloud.utils import cloud_storage


PROJECT_ID = 'project-id'
SERVICE_ACCOUNT_KEY_FILE = '/tmp/service_account_key.json'
SOURCE_FILE_PATH = '/tmp/file.txt'
DESTINATION_FILE_URL = 'gs://bucket_name/blob1/blob2/file.txt'

cloud_storage_utils = cloud_storage.CloudStorageUtils(PROJECT_ID,
                                                      SERVICE_ACCOUNT_KEY_FILE)
cloud_storage_utils.upload_file_to_url(SOURCE_FILE_PATH, DESTINATION_FILE_URL)
```

##### b. Upload files by specifying bucket and file path separately

If you have Cloud Storage bucket name and relative path to the file instead of a
fully formed Cloud Storage URL, use this method to upload file. The bucket will
be created if it doesn't exist.

```python
from gps_building_blocks.cloud.utils import cloud_storage


PROJECT_ID = 'project-id'
SERVICE_ACCOUNT_KEY_FILE = '/tmp/service_account_key.json'
SOURCE_FILE_PATH = '/tmp/file.txt'
BUCKET_NAME = 'bucket_name'
DESTINATION_FILE_PATH = 'blob1/blob2/file.txt'

cloud_storage_utils = cloud_storage.CloudStorageUtils(PROJECT_ID,
                                                      SERVICE_ACCOUNT_KEY_FILE)
cloud_storage_utils.upload_file(SOURCE_FILE_PATH,
                                BUCKET_NAME,
                                DESTINATION_FILE_PATH)
```

#### 4.2. Upload directory

Directory from local file system can be uploaded to Cloud Storage using one of
the following ways.

##### a. Upload directory by specifying Cloud Storage URL

If you have fully formed Cloud Storage URL for the destination in the form of
`gs://bucket_name/path/to/dir`, use this method to upload file. The bucket will
be created if it doesn't exist.

```python
from gps_building_blocks.cloud.utils import cloud_storage


PROJECT_ID = 'project-id'
SERVICE_ACCOUNT_KEY_FILE = '/tmp/service_account_key.json'
SOURCE_DIR_PATH = '/tmp/path/to/dir'
DESTINATION_DIR_URL = 'gs://bucket_name/path/to/dir'

cloud_storage_utils = cloud_storage.CloudStorageUtils(PROJECT_ID,
                                                      SERVICE_ACCOUNT_KEY_FILE)
cloud_storage_utils.upload_directory_to_url(SOURCE_DIR_PATH,
                                            DESTINATION_DIR_URL)
```

##### b. Upload directory by specifying bucket and directory path separately

If you have Cloud Storage bucket name and relative path to the directory instead
of a fully formed Cloud Storage URL, use this method to upload directory. The
bucket will be created if it doesn't exist.

```python
from gps_building_blocks.cloud.utils import cloud_storage


PROJECT_ID = 'project-id'
SERVICE_ACCOUNT_KEY_FILE = '/tmp/service_account_key.json'
SOURCE_DIR_PATH = '/tmp/path/to/dir'
BUCKET_NAME = 'bucket_name'
DESTINATION_DIR_PATH = 'blob1/blob2'

cloud_storage_utils = cloud_storage.CloudStorageUtils(PROJECT_ID,
                                                      SERVICE_ACCOUNT_KEY_FILE)
cloud_storage_utils.upload_directory(SOURCE_DIR_PATH,
                                     BUCKET_NAME,
                                     DESTINATION_DIR_PATH)
```
