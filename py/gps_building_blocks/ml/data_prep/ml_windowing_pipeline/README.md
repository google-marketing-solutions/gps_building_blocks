# ML Windowing Pipeline

The ML Windowing Pipeline (MLWP) creates ML-ready training data for learning how
to make user-level predictions about customer lifetime value, propensity to
purchase or churn, etc.

The input to MLWP can come from any BigQuery source, including Google Analytics,
Firebase, and/or any CRM data. For each user, MLWP will look at multiple
snapshots over time. For each snapshot, MLWP creates a lookback window of
historical features, and a prediction window containing the target label (e.g.
converted, churned, lifetime value etc).

The output of MLWP can be fed directly into an ML tool like AutoML. However, it
is generally advisable to inspect the intermediate data exploration output, to
check for discontinuities in the data, label leakage, and help select the best
parameters like lookback window size.

The main MLWP script is `run_end_to_end_pipeline.py`. This is equivalent to
running the following 4 pipelines in sequence:

* ***`run_data_extraction_pipeline.py`***:
Extracts conversion and session data from the specified analytics table. Use the
sample conversion and session SQL files in `templates/` to write your own custom
conversion and session data extraction definitions.
* ***`run_data_exploration_pipeline.py`***:
Extracts numeric and categorical facts into BigQuery tables for data exploration
and analysis. This can help find anomolous data and facts that might decrease
the performance of the machine learning algorithm. Also extracts user activity
snapshots, which can help in determining the best window size etc. Note that
`prediction_window_conversions_to_label.sql` must be overridden with the logic
to transform prediction window conversions into a machine learning label.
* ***`run_windowing_pipeline.py`***:
Segments the user data into multiple, potentially overlapping, time windows,
with each window containing a lookback window and a prediction window. By
default, the sliding_windows.sql algorithm is used. Use the parameter/flag
`--windows_sql` to replace this with a different algorithm, like session-based
windowing in `session_windows.sql`. Note that
`prediction_window_conversions_to_label.sql` must be overridden with the logic
to transform prediction window conversions into a machine learning label.
* ***`run_features_pipeline.py`***:
Generates features from the windows of data computed in
`run_windowing_pipeline.py`. By default, features are generated automatically.
For more precise feature generation, use the `--features_sql` parameter/flag to
point to the `features_from_input.sql` file (see the second example usage
below), or point to your own custom feature generation SQL script.

## Example Usage: Purchase Propensity for the Google Merchandise Store

### Automatic Feature Generation

```
python3 run_end_to_end_pipeline.py \
--project_id=<PROJECT_ID> \
--dataset_id=<DATASET_ID> \
--run_id=<OPTIONAL RUN_ID> \
--analytics_table="bigquery-public-data.google_analytics_sample.ga_sessions_*" \
--snapshot_start_date="2016-11-17" \
--snapshot_end_date="2017-07-01" \
--slide_interval_in_days=7 \
--lookback_window_size_in_days=30 \
--lookback_window_gap_in_days=0 \
--prediction_window_gap_in_days=1 \
--prediction_window_size_in_days=14 \
--stop_on_first_positive=True \
--top_n_values_per_fact=5
```

### Manual Feature Generation
```
python3 run_end_to_end_pipeline.py \
--project_id=<PROJECT_ID> \
--dataset_id=<DATASET_ID> \
--run_id=<OPTIONAL RUN_ID> \
--analytics_table="bigquery-public-data.google_analytics_sample.ga_sessions_*" \
--snapshot_start_date="2016-11-17" \
--snapshot_end_date="2017-07-01" \
--slide_interval_in_days=7 \
--lookback_window_size_in_days=30 \
--lookback_window_gap_in_days=0 \
--prediction_window_gap_in_days=1 \
--prediction_window_size_in_days=14 \
--stop_on_first_positive=True \
--features_sql='features_from_input.sql' \
--sum_values='totals_visits;totals_hits' \
--avg_values='totals_visits;totals_hits' \
--count_values='geoNetwork_metro:[Providence-New Bedford///,MA",Rochester-Mason City-Austin///,IA]:[Others]' \ # pylint: disable=line-too-long
--mode_values='hits_eCommerceAction_action_type:[3]:[Others]' \
--proportions_values='channelGrouping:[Organic Search,Social,Direct,Referral,Paid Search,Affiliates]:[Others]' \ # pylint: disable=line-too-long
--latest_values='device_isMobile:[false,true]:[Others]'
```
For more details on the parameters, see `run_end_to_end_pipeline.py` or the
individual pipelines. Also, MLWP can be run as a library using
`ml_windowing_pipline.py`.
