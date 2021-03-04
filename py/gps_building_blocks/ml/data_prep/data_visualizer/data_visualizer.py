# Copyright 2021 Google LLC
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

# python3
"""Common (gateway) API to execute all data visualization functions.

These functions are used to visualize Instances and Facts output by the
MLDataWindowingPipeline tool in the process of creating an ML-ready dataset.
For more info:
https://github.com/GoogleCloudPlatform/cloud-for-marketing/tree/master/marketing-analytics/predicting/ml-data-windowing-pipeline

The plots are saved to pdf files and uploaded to a specified GCS bucket.
"""

import typing
from google.cloud.bigquery import client
import matplotlib
from gps_building_blocks.cloud.utils import cloud_auth
from gps_building_blocks.cloud.utils import cloud_storage
from gps_building_blocks.ml.data_prep.data_visualizer import fact_visualizer
from gps_building_blocks.ml.data_prep.data_visualizer import instance_visualizer
from gps_building_blocks.ml.data_prep.data_visualizer import viz_utils


def visualize_instances(config_file: str) -> None:
  """Visualizes the statistics from the Instance table in BigQuery.

  This involves calculating statistics from the Instance table in BigQuery,
  generates and outputs plots into a pdf file and uploads the pdf file to
  a given location in Cloud Storage.

  Args:
    config_file: Path to the configuration file.
  """
  viz_config = viz_utils.parse_config_file(config_file)

  project_id = viz_config['project_id']
  dataset = viz_config['dataset']
  instance_table = viz_config['instance_table']
  instance_table_path = f'{project_id}.{dataset}.{instance_table}'
  bq_client = typing.cast(
      client.Client,
      cloud_auth.build_service_client(
          service_name='bigquery',
          service_account_credentials=cloud_auth.get_default_credentials()))
  storage_client = cloud_storage.CloudStorageUtils(
      project_id=viz_config['project_id'],
      service_account_key_file=viz_config['service_account_key_file'])
  pdf_output = matplotlib.backends.backend_pdf.PdfPages(
      viz_config['output_local_path'])

  ins_viz_obj = instance_visualizer.InstanceVisualizer(
      bq_client=bq_client,
      instance_table_path=instance_table_path,
      num_instances=viz_config['num_instances'],
      label_column=viz_config['label'],
      positive_class_label=viz_config['True'],
      negative_class_label=viz_config['False'])

  ins_viz_obj.plot_instances(**viz_config['plot_style_params'])
  pdf_output.savefig()
  storage_client.upload_file_to_url(viz_config['output_local_path'],
                                    viz_config['output_gcs_path'])


def visualize_facts(config_file: str) -> None:
  """Visualizes the statistics from the Facts table in BigQuery.

  This involves calculating statistics from the Facts table in BigQuery,
  generates and outputs plots into a pdf file and uploads the pdf file to
  a given location in Cloud Storage.

  Args:
    config_file: Path to the configuration file.
  """
  viz_config = viz_utils.parse_config_file(config_file)

  project_id = viz_config['project_id']
  dataset = viz_config['dataset']
  facts_table = viz_config['facts_table']
  facts_table_path = f'{project_id}.{dataset}.{facts_table}'
  bq_client = typing.cast(
      client.Client,
      cloud_auth.build_service_client(
          service_name='bigquery',
          service_account_credentials=cloud_auth.get_default_credentials()))
  storage_client = cloud_storage.CloudStorageUtils(
      project_id=viz_config['project_id'],
      service_account_key_file=viz_config['service_account_key_file'])
  pdf_output = matplotlib.backends.backend_pdf.PdfPages(
      viz_config['output_local_path'])

  fact_viz_obj = fact_visualizer.FactVisualizer(
      bq_client=bq_client,
      facts_table_path=facts_table_path,
      numerical_facts=viz_config['numerical_fact_list'],
      categorical_facts=viz_config['categorical_fact_list'],
      number_top_levels=viz_config['number_top_levels'])

  fact_viz_obj.plot_facts(**viz_config['plot_style_params'])
  pdf_output.savefig()
  storage_client.upload_file_to_url(viz_config['output_local_path'],
                                    viz_config['output_gcs_path'])
