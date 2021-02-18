# TCRM - Cloud Composer for Data

# TCRM

## What does TCRM do?

TCRM is a scalable solution, designed to get more clients to share their 1P data
, ML predictions and offline conversions with Google Platforms (GA, Ads, etc.).
TCRM runs on
[Cloud Composer](https://cloud.google.com/composer/), using
Apache [Airflow](https://airflow.apache.org/).

## TCRM [DAG](https://airflow.apache.org/docs/stable/concepts.html#dags)

*   `tcrm_bq_to_ga`: Transfer events from an SQL table in
    [BigQuery](https://cloud.google.com/bigquery/) to
    [Google Analytics](https://analytics.google.com/analytics/web/)

*   `tcrm_gcs_to_ga`: Transfer events from
    [Google Cloud Storage (GCS)](https://cloud.google.com/storage/)
    to Google Analytics. The events may be in a JSON or CSV formatted files in
    GCS.

NOTE: BigQuery/GCS to
[Google Ads UAC](https://developers.google.com/adwords/api/docs/guides/mobile-app-campaigns)
DAGs are currently under development.
