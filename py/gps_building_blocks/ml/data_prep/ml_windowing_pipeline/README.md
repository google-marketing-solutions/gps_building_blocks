# ML Windowing Pipeline

## Background

Businesses are increasingly aware of the value of sophisticated Machine Learning
(ML) models. Propensity Models, Customer Lifetime Value Models, Recommender
Systems, Response/Uplift Models, and Next Best Action/Offer Models can help
businesses acquire and retain customers through personalization.

However, extra work is often required to build such models from data stored in
tools such as GA360 or CRM systems. Although these tools can help businesses
organize and transform their data to meet business objectives at the aggregate
level, usually the data has to be re-engineered for ML modeling.

It usually takes about 70-80% (2-3 weeks) of the project time to create an ML
dataset from structured (tabular) datasets such as CRM, GA360 or Firebase, which
is the most time-consuming step of the end to end model building process.
Although the availability of the tools such as Google Cloud AutoML Tables could
cut down the model building time from weeks to hours, still the data preparation
step is the main bottleneck of this process. This makes the iteration of the ML
solution building process harder, which is an essential requirement of the ML
model development to get the optimal results based on different design choices.

### What is an ML Dataset?
Usually, creation of an ML dataset from customer behaviour data such as GA360,
Firebase ot CRM  involves:

* First creating a single data snapshot of users with respect to a given
  calendar date d. This snapshot consists of:
  * Instances: for example, all the users who have done some action in a
    website up until the date d.
  * Features: for each selected instance, aggregated behavior in a well-defined
    time period in the past from day d called lookback window.
  * Labels: for each selected instance, the value we would like to predict in a
    well-defined time period into the future from the day d (called prediction
    window) such as a binary value like purchased or not purchased (binary
    label), multinomial value like the name of the item purchased (multi-class
    label) or numerical value like the total value of all the products purchased
    (regression label).
* Second, generating a series of such snapshots over time to capture recency,
  frequency and changing behaviour of users, seasonality and other trends/events
  over time. This is vital in a period like Covid-19 to capture changing user
  behaviour which is also known as the Concept Drift. Also, with multiple
  snapshots, we would be able to generate more data for our ML model with
  limited original data.

## ML Windowing Pipeline

ML Windowing Pipeline creates an ML dataset by taking multiple data snapshots
over time in a very fast way. It has been built to run on Google Cloud BigQuery
and the input data is expected to be available as a BigQuery table. The
developer can simply specify the time-related inputs such as starting and ending
dates of the snapshots and sizes of the lookback, prediction and moving windows,
and variable names and aggregate functions to generate features and labels, when
using this module to create an ML dataset.

### Before running the module:
* Login to an existing Google Cloud project or create a new one.
* Make sure your CRM, GA360 or Firebase dataset is in a BigQuery table of this
  GCP project.
* Install the ml windowing pipeline:

 ```bash
  pip install gps_building_blocks
 ```

### Running:

Importing the module:

```python
from gps_building_blocks.ml.data_prep.ml_windowing_pipeline import ml_windowing_pipeline
```

Running the module involves the following 4 steps:

* Step 1: Run Data Extraction pipeline
* Step 2: Run Data Exploration pipeline (Optional)
* Step 3: Run Windowing pipeline
* Step 4: Run Feature Generation pipeline

These steps are explained below by using a publicly-available, sample GA360
dataset ([Google merchandize store data](https://support.google.com/analytics/answer/7586738?hl=en)
as the input dataset. However, the same process could be applied to Firebase or
CRM data with the right parameters.

## Step 1: Run Data Extraction Pipeline
This step extracts conversion and session data from the GA360 data table in
BigQuery. In order to run this step, first modify the corresponding conversion
and session SQL files in templates/ directory as explained below to specify the
label definition and select the GA variables for creating features later on.

### 1.1. Defining label definition
Defining the label involves the following two steps. This module supports a
binary, a multi-class or a regression label depending on how it is defined in
the following steps.

#### A. Selection of the label variable and value
In `templates/conversions_google_analytics.sql` file, you can specify the variable and the
value used to define the ML label. For example, the following code specifies the
variable and value used to define a binary label where the label is True when
the GA360 variable hits.eCommerceAction.action_type contains value ‘6’ and False
otherwise.

```sql
CREATE OR REPLACE TABLE `{{conversions_table}}`
AS (
  SELECT DISTINCT
    IFNULL(NULLIF(GaTable.clientId, ''), GaTable.fullVisitorId) AS user_id,
    TIMESTAMP_SECONDS(GaTable.visitStartTime) AS conversion_ts,
    TRUE AS label
  FROM
    `{{analytics_table}}` AS GaTable, UNNEST(GaTable.hits) as hits
  WHERE
    hits.eCommerceAction.action_type = '6'  -- Google Analytics code for "Completed purchase"
);
```

#### B. Selection of an aggregate function over the prediction window
Set `prediction_window_conversions_to_label_sql` with the filename of the SQL
file that contains the aggregation function used to calculate the ML label value 
over the prediction window period.

##### B.1 Binary Classification

Use `'prediction_window_conversions_to_label_binary.sql'`. This is the default
method. For example, the following SQL code (the default setting) can be used to
create a binary label for the prediction window where the label is assigned the 
value True whenever one or more purchases occurred in the prediction window and 
False otherwise.

```sql
IFNULL(
  (
    SELECT LOGICAL_OR(Conversions.label)
    FROM UNNEST(PredictionWindowConversions.conversions) AS Conversions
  ), FALSE)
```

##### B.2 Regression
Use `'prediction_window_conversions_to_label_regression.sql'`.  For example, the 
following SQL code (the default setting) can be used to create a regression 
label for the prediction window where the label is assigned the sum of all the 
purchases occurred in the prediction window.

```sql
IFNULL(
  (
    SELECT SUM(Conversions.label)
    FROM UNNEST(PredictionWindowConversions.conversions) AS Conversions
  ), 0)
```
More examples are given in the 
`templates/prediction_window_conversions_to_label_regression.sql` file on how to
create regression labels.

##### B.3 Other Methods

For other classification methods (e.g. multi-class), create a new template file
(e.g. prediction_window_conversions_to_label_multi_class.sql) and specify the
SQL fragment to assign label to a prediction window. Set 
`prediction_window_conversions_to_label_sql` to the template filename.

### 1.2. Defining session variable to extract
`templates/sessions_google_analytics.sql` file specifies how to extract the
GA360 session variables into an internal format called
facts. You can use the variables we selected in this file by default or modify
it to extract the session information you want to create features from. The
facts selected by default includes GA variables related to totals such as
*totals.visits, totals.hits, trafficSource, device, geoNetwork,
hits.page_views, hits.eventInfo and hits.latencyTracking*.

Every row/session in the Google Analytics BigQuery table has up to approximately
350 facts. For efficiency, you should only extract the data you need. More
instructions on how to use/modify the script is given in this SQL file itself.

`templates/sessions_firebase.sql` file provides the templates to extract the
eventParams value corresponding with event name and other information like
trafficSource and geo into an internal format called facts. The example events
and params are selected based on the list of recommended events as in
[here](https://support.google.com/analytics/answer/9267735?hl=en&ref_topic=9756175).
Please find the description for the existing extract functions in the template here:

| Function | Description |
|------------|-------------|
| ExtractEventParamStringValue | Extract string value from event_params. |
| ExtractEventParamNumericValue | Extract int, double or float value from event_params. |

### 1.3. Run data extraction
After the above steps, now you can run the data extraction pipeline. This
pipeline extracts the selected GA variables and labels and writes them into
interim tables in BigQuery for further processing. This step can be run as a
function call in a jupyter Notebook or in command line in the Google Cloud
project as follows:

**Usage example:**

Parameters:

| Parameter  | Description | Example |
|------------|-------------|---------|
| project_id  | Required. Google Cloud project to run on. | *'my_gcp_project'* |
| dataset_id  | Required. BigQuery dataset to write the output. Make sure this BigQuery dataset already exist. | *'mlwp_data'* |
| analytics_table | Required. Full BigQuery id of the Google Analytics/Firebase table. | *'bigquery-public-data.google_analytics_sample.ga_sessions_\*'* |
| conversions_sql | Optional. Name of the conversion extraction SQL file in templates/ directory. Default value is 'conversions_google_analytics.sql'. | *'my_analytics_conversions.sql'* |
| sessions_sql | Optional. Name of the session extraction SQL file in templates/ directory. Default value is 'sessions_google_analytics.sql'. | *'my_analytics_sessions.sql'* |
| run_id | Optional. Suffix for the output tables. Must be compatible with BigQuery table naming requirements. Note the same run_id must be used for all pipelines in the same run. Helpful to separate outputs in multiple runs. | *'01'*, *'20210301'* |
| verbose | Optional. Outputs sql commands being executed for debugging. Default value is False. | *True* |
| templates_dir | Optional. The path of the folder containing the user-overridden SQL templates. If a template is not found in the specified folder path, it will look at the MLWP templates folder. | */path/my_templates_folder* |

To run in jupyter notebook:

```python
params = {
 'project_id':'my_gcp_project',
 'dataset_id':'mlwp_data',
 'analytics_table':'bigquery-public-data.google_analytics_sample.ga_sessions_*',
 'conversions_sql':'conversions_google_analytics.sql',
 'sessions_sql':'sessions_google_analytics.sql',
 'run_id':'01'
}
ml_windowing_pipeline.run_data_extraction_pipeline(params)
```

To run in command line:

```bash
python run_data_extraction_pipeline.py \
--project_id='my_gcp_project' \
--dataset_id='mlwp_data' \
--analytics_table='bigquery-public-data.google_analytics_sample.ga_sessions_*' \
--conversions_sql='conversions_google_analytics.sql' \
--sessions_sql='sessions_google_analytics.sql' \
--run_id='01'
```

### Step 2: Run Data Exploration pipeline (Optional)
This step outputs facts and instances into BigQuery tables
(`numeric_facts_<run_id>`, `categorical_facts_<run_id>`  and
`instances_<run_id>`) for data exploration and analysis.
Output Instance BigQuery table contains all the instances (e.g. users) selected
for each snapshot date with some additional information such as their label,
days since the first activity and days since the last activity. This table can
be analysed to explore:

* the number of instances and the label distribution of snapshots over time
  (e.g. proportion of positive instances over time for the binary label): this
  would help us understand any discrepancies of the label (e.g. conversion rate)
  over time leading to dropping the data in a certain period from the analysis
  or seasonality and other trends over time giving insights on new features to
  add to the model to capture them.
* the distributions of `days_since_first_activity` and
  `days_since_latest_activity` could be used to determine the lookback window
  size and whether it makes sense to create the model only for more recent
  visitors.

The output facts table in BigQuery can be explored to understand the
distribution of numerical and categorical GA variables over time helping us to
identify any data issues and inconsistencies leading to select only the
consistent variables for feature generation. This is also helpful to select
inputs for feature generation such as top N meaningful categories for generating
features from a categorical variable.

[Data Visualizer](https://github.com/google/gps_building_blocks/tree/master/py/gps_building_blocks/ml/data_prep/data_visualizer)
python component can be used for these visualizations.

**Usage example:**

Parameters:

| Parameter  | Description | Example |
|------------|-------------|---------|
| project_id  | Required. Google Cloud project to run on. | *'my_gcp_project'* |
| dataset_id  | Required. BigQuery dataset to write the output. Make sure this BigQuery dataset already exist. | *'mlwp_data'* |
| analytics_table | Required. Full BigQuery id of the Google Analytics/Firebase table. | *'bigquery-public-data.google_analytics_sample.ga_sessions_\*'* |
| snapshot_start_date  | Required. The date of the first data snapshot. Format is YYYY-MM-DD. | *'2016-11-17'* |
| snapshot_end_date  | Required. The date of the last data snapshot. Format is YYYY-MM-DD. | *'2017-07-01'* |
| slide_interval_in_days | Required. Required. Number of days between successive snapshots. | *7* |
| prediction_window_gap_in_days | Required. The gap between the snapshot date and the prediction window start date in days. The prediction window starts on (snapshot date + prediction_window_gap_in_days). Conversions outside the prediction window are ignored. Minimum value is 1. | *1* |
| prediction_window_size_in_days | Required. Duration of the prediction window in days. The prediction window ends on (snapshot date + prediction_window_gap_in_days + prediction_window_size_in_days). Conversions outside the prediction window are ignored. Minimum value is 1. | 14 |
| prediction_window_conversions_to_label_sql | Optional. Default value is prediction_window_conversions_to_label_binary.sql. Name of the SQL file that converts an array of conversions into a label extraction SQL file in templates directory . | *'prediction_window_conversions_to_label_regression.sql'* |
| numeric_facts_sql | Optional. Default value is numeric_facts.sql. Name of the SQL file to generate numerical facts table that will be later used in data visualizer. Firebase data is recommended to use numeric_facts_firebase.sql to generate all numeric facts | numeric_facts_firebase.sql |
| categorical_facts_sql | Optional. Default value is categorical_facts.sql. Name of the SQL file to generate categorical facts table that will be later used in data visualizer. Firebase data is recommended to use categorical_facts_firebase.sql to generate all numeric facts | categorical_facts_firebase.sql |
| timezone | Optional. Timezone for Google Analytics Data. Default value is UTC. | *'Australia/Sydney'* |
| run_id | Optional. Suffix for the output tables. Must be compatible with BigQuery table naming requirements. Note the same run_id must be used for all pipelines in the same run. Helpful to separate outputs in multiple runs. | *'01'*, *'20210301'*  |
| verbose | Optional. Outputs sql commands being executed for debugging. Default value is False. | *True* |

To run in jupyter Notebook:

```python
params = {
 'project_id':'my_gcp_project',
 'dataset_id':'mlwp_data',
 'analytics_table':'bigquery-public-data.google_analytics_sample.ga_sessions_*',
 'snapshot_start_date':'2016-11-17',
 'snapshot_end_date':'2017-07-01',
 'slide_interval_in_days':7,
 'prediction_window_gap_in_days':1,
 'prediction_window_size_in_days':14,
 'run_id':'01'
}
ml_windowing_pipeline.run_data_exploration_pipeline(params)
```

To run in command line:

```bash
python run_data_exploration_pipeline.py \
--project_id='my_gcp_project' \
--dataset_id='mlwp_data' \
--analytics_table='bigquery-public-data.google_analytics_sample.ga_sessions_*' \
--snapshot_start_date='2016-11-17' \
--snapshot_end_date='2017-07-01' \
--slide_interval_in_days=7 \
--prediction_window_gap_in_days=1 \
--prediction_window_size_in_days=14 \
--run_id='01'
```

### Step 3. Run Data Windowing Pipeline
This step segments the user data into multiple, potentially overlapping time
windows, with each window containing a lookback window and a prediction window.
This generates an internal table in BigQuery (`windows_<run_id>`) for further
processing.

The windows can be defined in two ways:

* based on calendar dates and a sliding window. This is implemented in the
  `sliding_windows.sql` and used as the default.
* based on each session of each user. This is implemented in the
  `session_windows.sql` and you can use the `windows_sql` parameter to specify it.

**Usage example:**

| Parameter  | Description | Example |
|------------|-------------|---------|
| project_id  | Required. Google Cloud project to run on. | *'my_gcp_project'* |
| dataset_id  | Required. BigQuery dataset to write the output. Make sure this BigQuery dataset already exist. | *'mlwp_data'* |
| snapshot_start_date  | Required. The date of the first data snapshot. Format is YYYY-MM-DD. | *'2016-11-17'* |
| snapshot_end_date  | Required. The date of the last data snapshot. Format is YYYY-MM-DD. | *'2017-07-01'* |
| slide_interval_in_days | Required. Required. Number of days between successive snapshots. | *7* |
| prediction_window_gap_in_days | Required. The gap between the snapshot date and the prediction window start date in days. The prediction window starts on ( snapshot date + prediction_window_gap_in_days). Conversions outside the prediction window are ignored. Minimum value is 1. | *1* |
| prediction_window_size_in_days | Required. Duration of the prediction window in days. The prediction window ends on (snapshot date + prediction_window_gap_in_days + prediction_window_size_in_days). Conversions outside the prediction window are ignored. Minimum value is 1. | *14* |
| lookback_window_gap_in_days | Required. The gap between the snapshot date and the end of the lookback window in days. The lookback window ends on (snapshot date - lookback_window_gap_in_days). Sessions outside the lookback window are ignored. | *1* |
| lookback_window_size_in_days | Required. Duration of the prediction window in days. The lookback window starts on (snapshot date - lookback_window_size_in_days - lookback_window_gap_in_days). Sessions outside the feature window are ignored. | *30* |
| stop_on_first_positive | Optional. Stop considering the user for future snapshots after the first positive label. Default value is False. | *True* |
| windows_sql | Optional. Name of the windows extraction SQL file in templates/ directory. Default value is 'sliding_windows.sql'. Set this to 'session_windows.sql' to window data based on user session. | *'sliding_windows.sql'* |
| prediction_window_conversions_to_label_sql | Optional. Default value is prediction_window_conversions_to_label_binary.sql. Name of the SQL file that converts an array of conversions into a label extraction SQL file in templates directory . | *'prediction_window_conversions_to_label_regression.sql'* |
| timezone | Optional. Timezone for Google Analytics Data. Default value is UTC. | *'Australia/Sydney'* |
| run_id | Optional. Suffix for the output tables. Must be compatible with BigQuery table naming requirements. Note the same run_id must be used for all pipelines in the same run. Helpful to separate outputs in multiple runs. | *'01'*, *'20210301'*  |
| verbose | Optional. Outputs sql commands being executed for debugging. Default value is False. | *True* |
| templates_dir | Optional. The path of the folder containing the user-overridden SQL templates. If a template is not found in the specified folder path, it will look at the MLWP templates folder. | */path/my_templates_folder* |

To run in jupyter Notebook:

```python
params = {
 'project_id':'my_gcp_project',
 'dataset_id':'mlwp_data',
 'snapshot_start_date':'2016-11-17',
 'snapshot_end_date':'2017-07-01',
 'slide_interval_in_days':7,
 'prediction_window_gap_in_days':1,
 'prediction_window_size_in_days':14,
 'lookback_window_gap_in_days':1,
 'lookback_window_size_in_days':30,
 'run_id':'01'
}
ml_windowing_pipeline.run_windowing_pipeline(params)
```

To run in command line:

```bash
python main.py \
--project_id='my_gcp_project' \
--dataset_id='mlwp_data' \
--snapshot_start_date='2016-11-17' \
--snapshot_end_date='2017-07-01' \
--prediction_window_gap_in_days=1 \
--prediction_window_size_in_days=14 \
--slide_interval_in_days=7 \
--lookback_window_size_in_days=30 \
--lookback_window_gap_in_days=0 \
--stop_on_first_positive=True
--run_id='01'
```

### Step 4. Run Features Pipeline
This final step generates features from the windows of data computed in Pipeline
3 and outputs to `features_<run_id>` table in BigQuery.

The features could be generated in 2 ways:

* Feature generation with manual input (semi-automated):

  In this option, you’ll have more control of selecting the GA variables,
  feature types (aggregation functions such as Min, Max, Average for numerical
  variables and Proportions, Counts, Mode for categorical variables) and input
  values (e.g. selected category levels to create features from a categorical
  variable) based on prior knowledge or the exploration of facts in step 2.

**Usage example:**

Parameters:

| Parameter  | Description | Example |
|------------|-------------|---------|
| project_id  | Required. Google Cloud project to run on. | *'my_gcp_project'* |
| dataset_id  | Required. BigQuery dataset to write the output. Make sure this BigQuery dataset already exist. | *'mlwp_data'* |
| features_sql | Required. Name of the feature extraction SQL file in templates/ directory. Use 'features_from_input.sql' for  extracting features based on input operation parameters. | *'features_from_input.sql'* |
| sum_values | Optional. A semi-colon separated list of numerical fact names to create Sum features (sum of all the values over the lookback window). | *'totals_visits;totals_hits'* |
| avg_values | Optional. A semi-colon separated list of numerical fact names to create Average features (average of all the values over the lookback window). | *'totals_visits;totals_hits'* |
| min_values | Optional. A semi-colon separated list of numerical fact names to create Minimum features (minimum of all the values over the lookback window). | *'totals_visits;totals_hits'* |
| max_values | Optional. A semi-colon separated list of numerical fact names to create Maximum features (maximum of all the values over the lookback window). | *'totals_visits;totals_hits'* |
| count_values | Optional. A semi-colon separated list of categorical Feature Options to create Count feature (total occurance of each category): `<feature_option1>;<feature_option2>;<feature_option3>`. Each Feature Option should contain a categorical fact name, a list of values to consider and a default value. The default value is specified to use the as the common value for any value not on the provided list. Feature Option = `<fact_name>:[<value1>, …,<valueN>]:[<default_value>]` | *'channelGrouping:[Organic Search,Social,Direct,Referral,Paid Search,Affiliates]:[Other];device_isMobile:[false,true]:[Other]'* |
| proportions_values | Optional. A semi-colon separated list of categorical Feature Options to create Proportion feature (proportion of occurance of each category): `<feature_option1>;<feature_option2>;<feature_option3>`. Each Feature Option should contain a categorical fact name, a list of values to consider and a default value. The default value is specified to use the as the common value for any value not on the provided list. Feature Option = `<fact_name>:[<value1>, …,<valueN>]:[<default_value>]` | *'channelGrouping:[Organic Search,Social,Direct,Referral,Paid Search,Affiliates]:[Other];device_isMobile:[false,true]:[Other]'* |
| latest_values | Optional. A semi-colon separated list of categorical Feature Options to create Latest Value feature (the latest category value): `<feature_option1>;<feature_option2>;<feature_option3>`. Each Feature Option should contain a categorical fact name, a list of values to consider and a default value. The default value is specified to use the as the common value for any value not on the provided list. Feature Option = `<fact_name>:[<value1>, …,<valueN>]:[<default_value>]` | *'channelGrouping:[Organic Search,Social,Direct,Referral,Paid Search,Affiliates]:[Other];device_isMobile:[false,true]:[Other]'* |
| mode_values | Optional. A semi-colon separated list of categorical Feature Options to create Mode Value feature (the most frequent category value): `<feature_option1>;<feature_option2>;<feature_option3>`. Each Feature Option should contain a categorical fact name, a list of values to consider and a default value. The default value is specified to use the as the common value for any value not on the provided list. Feature Option = `<fact_name>:[<value1>, …,<valueN>]:[<default_value>]` | *'channelGrouping:[Organic Search,Social,Direct,Referral,Paid Search,Affiliates]:[Other];device_isMobile:[false,true]:[Other]'* |
| run_id | Optional. Suffix for the output tables. Must be compatible with BigQuery table naming requirements. Note the same run_id must be used for all pipelines in the same run. Helpful to separate outputs in multiple runs. | *'01'*, *'20210301'* |
| verbose | Optional. Outputs sql commands being executed for debugging. Default value is False. | *True* |
| templates_dir | Optional. The path of the folder containing the user-overridden SQL templates. If a template is not found in the specified folder path, it will look at the MLWP templates folder. | */path/my_templates_folder* |

To run in jupyter notebook:

```python
params = {
 'project_id':'my_gcp_project',
 'dataset_id':'mlwp_data',
 'features_sql':'features_from_input.sql',
 'sum_values':'totals_visits;totals_pageviews',
 'avg_values':'totals_visits;totals_pageviews',
 'min_values':'totals_visits;totals_pageviews',
 'max_values':'totals_visits;totals_pageviews',
 'count_values':'trafficSource_medium:[cpm,cpc,referral,affiliate,organic]:[Other];device_isMobile:[false,true]:[Other]',
 'latest_values':'trafficSource_medium:[cpm,cpc,referral,affiliate,organic]:[Other];device_isMobile:[false,true]:[Other]',
 'proportions_values':'trafficSource_medium:[cpm,cpc,referral,affiliate,organic]:[Other];device_isMobile:[false,true]:[Other]',
 'mode_values':'trafficSource_medium:[cpm,cpc,referral,affiliate,organic]:[Other];device_isMobile:[false,true]:[Other]',
 'run_id':'01',
}
ml_windowing_pipeline.run_features_pipeline(params)
```

To run in command line:

```bash
python run_features_pipeline.py \
--project_id='my_gcp_project' \
--dataset_id='mlwp_data' \
--features_sql='features_from_input.sql'\
--sum_values='totals_visits;totals_hits' \
--avg_values='totals_visits;totals_hits' \
--min_values='totals_visits;totals_hits' \
--max_values='totals_visits;totals_hits' \
--count_values='trafficSource_medium:[cpm,cpc,referral,affiliate,organic]:[Other];device_isMobile:[false,true]:[Other]' \
--proportions_values='trafficSource_medium:[cpm,cpc,referral,affiliate,organic]:[Other];device_isMobile:[false,true]:[Other]' \
--latest_values='trafficSource_medium:[cpm,cpc,referral,affiliate,organic]:[Other];device_isMobile:[false,true]:[Other]' \
--mode_values='trafficSource_medium:[cpm,cpc,referral,affiliate,organic]:[Other];device_isMobile:[false,true]:[Other]'
```

* Automated feature generation:

  This option applies all the supported feature types (aggregation functions)
  for all numerical and categorical facts to generate the features. For
  categorical features, top_n_values_per_fact value can be set to select the
  top n category values automatically to generate the features. This step is easier
  to set up to run, but you have to be careful of filtering out invalid features
  at the end as some feature types may not be valid for some variables.
  For example, it doesn’t make sense to create categorical feature types such as
  count_values, mode_values and proportions_values for GA variables related to
  device, operating system and browser as each cookie id has only one value for
  these variables.

**Usage example:**

| Parameter  | Description | Example |
|------------|-------------|---------|
| project_id  | Required. Google Cloud project to run on. | *'my_gcp_project'* |
| dataset_id  | Required. BigQuery dataset to write the output. Make sure this BigQuery dataset already exist. | *'mlwp_data'* |
| features_sql | Required. Name of the feature extraction SQL file in templates/ directory. Usee 'automatic_features.sql'. Check, modify and use 'features_google_analytics.sql' and 'features_firebase.sql' for custom SQL feature extraction. | *'automatic_features.sql'* |
| top_n_values_per_fact | Optional. Extract the top n values by count for each categorical fact to turn into features in automatic feature extraction. Default value is 3. | *5* |
| run_id | Optional. Suffix for the output tables. Must be compatible with BigQuery table naming requirements. Note the same run_id must be used for all pipelines in the same run. Helpful to separate outputs in multiple runs. | *'01'*, *'20210301'*  |
| verbose | Optional. Outputs sql commands being executed for debugging. Default value is False. | *True* |
| templates_dir | Optional. The path of the folder containing the user-overridden SQL templates. If a template is not found in the specified folder path, it will look at the MLWP templates folder. | */path/my_templates_folder* |

To run in jupyter notebook:

```python
params = {
 'project_id':'my_gcp_project',
 'dataset_id':'mlwp_data',
 'features_sql':'automatic_features.sql',
 'top_n_values_per_fact':5,
 'run_id':'01'
}
ml_windowing_pipeline.run_features_pipeline(params)
```

To run in command line:

```bash
python run_features_pipeline.py \
--project_id='my_gcp_project' \
--dataset_id='mlwp_data' \
--features_sql='automatic_features.sql' \
--top_n_values_per_fact='01' \
--run_id='01'
```

### Running End to End:

Alternatively, the four pipelines can be run end to end in command line as follows:

With manual feature generation:

```python
python3 run_end_to_end_pipeline.py \
--project_id='my_gcp_project' \
--dataset_id='mlwp_data' \
--run_id='01' \
--analytics_table='bigquery-public-data.google_analytics_sample.ga_sessions_*' \
--snapshot_start_date='2016-11-17' \
--snapshot_end_date='2017-07-01' \
--slide_interval_in_days=7 \
--lookback_window_size_in_days=30 \
--lookback_window_gap_in_days=1 \
--prediction_window_gap_in_days=1 \
--prediction_window_size_in_days=14 \
--stop_on_first_positive=True \
--features_sql='features_from_input.sql' \
--sum_values='totals_visits;totals_hits' \
--avg_values='totals_visits;totals_hits' \
--min_values='totals_visits;totals_hits' \
--max_values='totals_visits;totals_hits' \
--count_values='channelGrouping:[Organic Search,Social,Direct,Referral,Paid Search,Affiliates]:[Other]'
--mode_values='hits_eCommerceAction_action_type:[3]:[Others]' \
--proportions_values='channelGrouping:[Organic Search,Social,Direct,Referral,Paid Search,Affiliates]:[Others]'
--latest_values='device_isMobile:[false,true]:[Others]'
```

With automated feature generation:

```python
python3 run_end_to_end_pipeline.py \
--project_id='my_gcp_project' \
--dataset_id='mlwp_data' \
--run_id='01' \
--analytics_table='bigquery-public-data.google_analytics_sample.ga_sessions_*' \
--snapshot_start_date='2016-11-17' \
--snapshot_end_date='2017-07-01' \
--slide_interval_in_days=7 \
--lookback_window_size_in_days=30 \
--lookback_window_gap_in_days=1 \
--prediction_window_gap_in_days=1 \
--prediction_window_size_in_days=14 \
--stop_on_first_positive=True \
--top_n_values_per_fact=5
```

### Customizing MLWP for your Application:
MLWP uses several SQL templates and you might want to modify these templates
based on your label definition, label value, custom features, etc. You can
maintain your own copy of the templates in your own template directory outside
MLWP library.

- Execute `pip show gps_building_blocks` to get the location (\<location\>) of the package.
- Copy the templates you want to modify to your template folder (e.g. /path/my_templates_folder/)
- Modify the templates as you see fit and instructed in the steps below.
- Remember to provide the `template_dir` parameter in the MLWP pipelines. Example:

```python
params = {
 'project_id':'my_gcp_project',
 'dataset_id':'mlwp_data',
 'features_sql':'features_from_input.sql',
 'sum_values':'totals_visits;totals_pageviews',
 'avg_values':'totals_visits;totals_pageviews',
 'min_values':'totals_visits;totals_pageviews',
 'max_values':'totals_visits;totals_pageviews',
 'count_values':'trafficSource_medium:[cpm,cpc,referral,affiliate,organic]:[Other];device_isMobile:[false,true]:[Other]',
 'latest_values':'trafficSource_medium:[cpm,cpc,referral,affiliate,organic]:[Other];device_isMobile:[false,true]:[Other]',
 'proportions_values':'trafficSource_medium:[cpm,cpc,referral,affiliate,organic]:[Other];device_isMobile:[false,true]:[Other]',
 'mode_values':'trafficSource_medium:[cpm,cpc,referral,affiliate,organic]:[Other];device_isMobile:[false,true]:[Other]',
 'run_id':'01',
 'templates_dir':'/path/my_templates_folder',
}
ml_windowing_pipeline.run_features_pipeline(params)
```

### Running Prediction Pipeline:

Before running this pipeline, first run the end-to-end windowing pipeline, and
then use the data to train an ML model. Once the model is deployed and you want
predictions about live customers, run this pipeline to generate features for the
customers over a single window of data, and then input the features into the
ML model to get it's predictions.

With manual feature generation:

```python
python run_prediction_pipeline.py \
--project_id=<PROJECT_ID> \
--dataset_id=<DATASET_ID> \
--analytics_table="bigquery-public-data.google_analytics_sample.ga_sessions_*" \
--snapshot_date_offset_in_days=1 \
--lookback_window_size_in_days=30 \
--lookback_window_gap_in_days=0 \
--features_sql='features_from_input.sql' \
--sum_values='totals_visits;totals_hits' \
--avg_values='totals_visits;totals_hits' \
--min_values='totals_visits;totals_hits' \
--max_values='totals_visits;totals_hits' \
--count_values='geoNetwork_metro:[Providence-New Bedford///,MA",Rochester-Mason City-Austin///,IA]:[Others]' \
--mode_values='hits_eCommerceAction_action_type:[3]:[Others]' \
--proportions_values='channelGrouping:[Organic Search,Social,Direct,Referral,Paid Search,Affiliates]:[Others]' \
--latest_values='device_isMobile:[false,true]:[Others]'
```

With automated feature generation:

```python
python run_prediction_pipeline.py \
--project_id=<PROJECT_ID> \
--dataset_id=<DATASET_ID> \
--analytics_table="bigquery-public-data.google_analytics_sample.ga_sessions_*" \
--snapshot_date_offset_in_days=1 \
--lookback_window_size_in_days=30 \
--lookback_window_gap_in_days=0 \
--categorical_fact_value_to_column_name_table=<BIGQUERY TABLE from training run>
```
