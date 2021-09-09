# Experimental media design utils

This directory contains functions that help to design media experiments.

## Install

```bash
pip install gps_building_blocks
```

## A/B Testing Experimental Design Module

This module contains functions that can be used to design statistically sound
media experiments to activate Propensity Models built using GA360, Firebase or
CRM data and assess the statistical significance of the results of such
experiments. It is crucial to design and estimates impact of media campaigns
using valid statistical methods to make sure the limited experimentation budget
is utilized effectively and to set the right expectations of the campaign
outcome.

### Calculate Statistical Sample Size for Proportions

`ab_testing_design.calc_chisquared_sample_size()` estimates the minimum sample
size required for either the Test or the Control group in an A/B test when the
KPI is a proportion such as the conversion rate by using the
[Chi-squared test](https://en.wikipedia.org/wiki/Chi-squared_test) of
proportions.

**Usage example:**

```python
from gps_building_blocks.analysis.exp_design import ab_testing_design

ab_testing_design.calc_chisquared_sample_size(
    baseline_conversion_rate_percentage=5,
    expected_uplift_percentage=10,
    power_percentage=80,
    confidence_level_percentage=90)
```

**Expected output:** minimum statistical sample size required for either the
Test or the Control group.

### Calculate Statistical Sample Sizes for Different Bins of Predicted Probabilities

One way to use the output from a Propensity Model to optimize marketing is to
first define different audience groups based on the predicted probabilities
(such as `High`, `Medium` and `Low` propensity groups) and then test the same or
different marketing strategies with those.

`ab_testing_design.calc_chisquared_sample_sizes_for_bins()` uses
`ab_testing_design.calc_chisquared_sample_size()` to estimate the statistical
sample sizes required for different groups (bins) of the predicted
probabilities based on different combinations of the `expected
uplift`, `statistical power` and `statistical confidence levels` specified as
input parameters.

**Usage example:**

```python
from gps_building_blocks.analysis.exp_design import ab_testing_design

ab_testing_design.calc_chisquared_sample_sizes_for_bins(
    labels=prediction_df['label'].values)
    probability_predictions=prediction_df['predictions'].values),
    number_bins=3,
    uplift_percentages=[5, 10, 15],
    power_percentages=[80, 90],
    confidence_level_percentages=[90, 95])
```

**Expected output:** a Pandas Dataframe with the following columns containing
statistical sample size for each bin for each combination of uplift_percentage,
statistical power and statistical confidence level.

| bin_number | bin_size | min_probability | conv_rate_percentage | uplift_percentage | power_percentage | confidence_level_percentage | required_sample_size |
|------------|----------|-----------------|----------------------|-------------------|------------------|-----------------------------|----------------------|
| 1          | 25000    | 0.235           | 20.1                 |  5                | 80               | 90                          | 22257                |
| 1          | 25000    | 0.235           | 20.1                 | 10                | 80               | 90                          |  5565                |
| 1          | 25000    | 0.235           | 20.1                 | 15                | 80               | 90                          |  2473                |
...

### Calculate Statistical Sample Size for the Top X% of Predicted Probabilities

Another way to use the output from a Propensity Model to optimize marketing is
to target the top X% of users having the highest predicted probability in a
remarketing campaign or an acquisition campaigns with the similar audience
strategy.

`ab_testing_design.calc_chisquared_sample_sizes_for_cumulative_bins()` uses
`ab_testing_design.calc_chisquared_sample_size()` to estimate the statistical
sample sizes required for different cumulative groups (bins) of the predicted
probabilities (top X%, top 2X% and so on) based on different combinations of
the `expected uplift`, `statistical power` and `statistical confidence levels`
specified as input parameters.

**Usage example:**

```python
from gps_building_blocks.analysis.exp_design import ab_testing_design

ab_testing_design.calc_chisquared_sample_sizes_for_cumulative_bins(
    labels=prediction_df['label'].values)
    probability_predictions=prediction_df['predictions'].values),
    number_bins=10,
    uplift_percentages=[5, 10, 15],
    power_percentages=[80, 90],
    confidence_level_percentages=[90, 95])
```

**Expected output:** a Pandas Dataframe with the following columns containing
statistical sample size for each cumulative bin for each combination of
uplift_percentage, statistical power and statistical confidence level.

| cumulative_bin_number | bin_size | bin_size_percentage | min_probability | conv_rate_percentage | uplift_percentage | power_percentage | confidence_level_percentage | required_sample_size |
|-----------------------|----------|---------------------|-----------------|----------------------|-------------------|------------------|-----------------------------|----------------------|
| 1                     | 25000    | 10                  | 0.235           | 20.1                 |  5                | 80               | 90                          | 22257                |
| 1                     | 25000    | 10                  | 0.183           | 20.1                 | 10                | 80               | 90                          |  5565                |
| ...                   | ...      | ...                 | ...             | ...                  | ...               | ...              | ...                         | ...                  |
| 2                     | 50000    | 20                  | 18.5            | 17.8                 |  5                | 80               | 90                          | 32855                |
...

### Performs Chi-squared Statistical Significance Test

`calc_chisquared_pvalue()` function performs the [Chi-squared test](https://en.wikipedia.org/wiki/Chi-squared_test)
to assess the statistical significance of the differences of proportions
among different experimental groups (e.x. different conversion rates among
Test and Control groups). For example, if the returned p-value is less than 0.05
we can conclude that the differences of the proportions under comparison are
statistically significant at 95% confidence level.

**Usage example:**

```python
from gps_building_blocks.analysis.exp_design import ab_testing_analysis

ab_testing_analysis.calc_chisquared_pvalue(
    group_counts=(10000, 15000),
    converter_counts=(1000, 1700))
```

**Expected output:** p-value of the test.
