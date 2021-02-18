# TCRM

_Copyright 2021 Google LLC. This solution, including any related sample code or
data, is made available on an “as is,” “as available,” and “with all faults”
basis, solely for illustrative purposes, and without warranty or representation
of any kind. This solution is experimental, unsupported and provided solely for
your convenience. Your use of it is subject to your agreements with Google, as
applicable, and may constitute a beta feature as defined under those agreements.
To the extent that you make any data available to Google in connection with your
use of the solution, you represent and warrant that you have all necessary and
appropriate rights, consents and permissions to permit Google to use and process
that data. By using any portion of this solution, you acknowledge, assume and
accept all risks, known and unknown, associated with its usage, including with
respect to your deployment of any portion of this solution in your systems, or
usage in connection with your business, if at all._

## Overview

TCRM (TaglessCRM) is a lightweight, automated data transfer library for clients who want to upload their marketing data to Google Platforms. It guarantees sending the data once and only once, and removes the hustle of dealing directly with the Google APIs.

Please refer to the following documentation for how to use this solution.

1.  [Installation Guide](./docs/install-guide.md)


### Glossary of Terms

Term                                                                        | Definition
--------------------------------------------------------------------------- | ----------
TCRM                                                                        | TaglessCRM.
Cloud Composer                                                              | A fully managed workflow orchestration service built on Apache Airflow.
Airflow                                                                     | Apache Airflow is an open-source workflow management platform.
Airflow DAG                                                                 | In Airflow, a DAG -- or a Directed Acyclic Graph -- is a collection of all the tasks you want to run, organized in a way that reflects their relationships and dependencies. A DAG is defined in a Python script, which represents the DAGs structure (tasks and their dependencies) as code.
Airflow Operator                                                            | An operator describes a single task in a workflow.
Airflow Hook                                                                | Hooks are interfaces to external platforms and databases like Hive, S3, MySQL, Postgres, HDFS, and Pig. Hooks implement a common interface when possible, and act as a building block for operators.
Google Ads                                                                  | Google Ads is an online advertising platform developed by Google, where advertisers bid to display brief advertisements, service offerings, product listings, or videos to web users. It can place ads both in the results of search engines like Google Search and on non-search websites, mobile apps, and videos
Google Analytics                                                            | Google Analytics is a web analytics service offered by Google that tracks and reports website traffic, currently as a platform inside the Google Marketing Platform brand.
Campaign Manager                                                            | Campaign Manager 360 is a web-based ad management system for advertisers and agencies. It helps you manage your digital campaigns across websites and mobile. This includes a robust set of features for ad serving, targeting, verification, and reporting.
BigQuery                                                                    | BigQuery is a fully-managed, serverless data warehouse that enables scalable analysis over petabytes of data.
Cloud Storage                                                               | Cloud Storage has an ever-growing list of storage bucket locations where you can store your data with multiple automatic redundancy options.

## DAGs

DAG            | Definition
-------------- | ----------
bq_to_ads_cm   | BigQuery To Google Ads Customer Match
bq_to_ads_oc   | BigQuery To Google Ads Offline Conversion
bq_to_ads_uac  | BigQuery To Google Ads Universal App Campaign
bq_to_cm       | BigQuery To Customer Match
bq_to_ga       | BigQuery To Google Analytics
gcs_to_ads_cm  | Google Cloud Storage To Google Ads Customer Match
gcs_to_ads_oc  | Google Cloud Storage To Google Ads Offline Conversion
gcs_to_ads_uac | Google Cloud Storage To Google Ads Universal App Campaign
gcs_to_cm      | Google Cloud Storage To Customer Match
gcs_to_ga      | Google Cloud Storage To Google Analytics

