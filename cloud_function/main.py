# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A Cloud Function that triggers the CoCoA Dataflow pipeline."""
import datetime
import os
from typing import Any, Dict, Sequence

from google.cloud import bigquery
from google.cloud import storage
from googleapiclient.discovery import build

# TODO(): Update the project and dataflow parameters below.
# Project and bigquery related parameters.
_PROJECT = 'cocoa'
_GCS_BUCKET = 'cocoa-df'
_BIGQUERY_LOCATION = 'EU'
_DATAFLOW_REGION = 'europe-west3'
_DATAFLOW_SUBNET = 'default'
_DATE_COLUMN = 'conversion_date'
_TABLE_NAME_NOCONSENT = 'pipeline_test.noconsent'
_INPUT_FILE_PATH = 'input'
_LOOKBACK_WINDOW = 1

# Dataflow parameters.
JOB = 'cocoa-cloud-function-test'
TEMPLATE = f'gs://{_GCS_BUCKET}/templates/cocoa-template'
PARAMETERS = {
    'metric': 'manhattan',
    'number_nearest_neighbors': '1',
}
ENVIRONMENT = {
    'subnetwork':
        f'https://www.googleapis.com/compute/v1/projects/{_PROJECT}/regions/{_DATAFLOW_REGION}/subnetworks/{_DATAFLOW_SUBNET}',
}


def run(
    event: Dict[str, Any], context: Any  # Cloud Function so pylint: disable=unused-argument
) -> None:
  """Background Cloud Function to be triggered by Cloud Logging.

  Prepares input data for the Dataflow pipeline and then triggers the pipeline.

  Args:
    event : The dictionary with data specific to this type of event. For
      details, see:
      https://cloud.google.com/functions/docs/samples/functions-log-stackdriver#code-sample
    context: Metadata of triggering event.

  Raises:
    RuntimeError: A dependency was not found, requiring this CF to exit.
      The RuntimeError is not raised explicitly in this function but is default
      behavior for any Cloud Function.

  Returns:
      None.
  """
  _prepare_pipeline_input()

  dataflow = build('dataflow', 'v1b3')
  request = dataflow.projects().locations().templates().launch(
      projectId=_PROJECT,
      gcsPath=TEMPLATE,
      location=_DATAFLOW_REGION,
      body={
          'jobName': JOB,
          'parameters': PARAMETERS,
          'environment': ENVIRONMENT,
      })

  request.execute()


def _prepare_pipeline_input() -> None:
  """Prepares dates to be processed for the CoCoA Dataflow pipeline.

  The CoCoA Dataflow pipeline requires an input file containing dates to
  process. This function prepares this file and uploads it to a Cloud Storage
  bucket.
  """
  latest_date = _get_latest_date_from_bigquery(_TABLE_NAME_NOCONSENT,
                                               _BIGQUERY_LOCATION, _PROJECT,
                                               _DATE_COLUMN)
  dates_to_process = _get_dates_to_process(latest_date, _LOOKBACK_WINDOW)

  # Prepare string of newline separated dates and write to GCS
  write_to_gcs(_GCS_BUCKET, _INPUT_FILE_PATH, 'dates.txt',
               '\n'.join(map(datetime.date.isoformat, dates_to_process)))


def _get_dates_to_process(
    start_date: str,
    lookback_window: int) -> Sequence[datetime.date]:
  """Generates a sequence of dates.

  Creates a sequence of dates based on the given start date and lookback window.

  Args:
    start_date: Starting date for the sequence.
    lookback_window: Number of days in the past.

  Returns:
    A sequence of dates ranging from start_date - lookback_window to the
    start_date.
  """
  return [
      datetime.date.fromisoformat(start_date) -
      datetime.timedelta(days=delta) for delta in range(lookback_window)
  ]


def _get_latest_date_from_bigquery(table_name: str, location: str, project: str,
                                   date_column: str) -> str:
  """Gets the latest date from a date column in a BigQuery table."""
  bq_client = bigquery.Client(location=location, project=project)
  query = f"""
          SELECT FORMAT_DATETIME("%F", MAX({date_column})) AS input_date
          FROM `{table_name}`
          """
  results_iter = bq_client.query(query).result()
  # There is only one row as we query by max(), which we now return.
  return next(results_iter).input_date


def write_to_gcs(bucket_name: str, path: str, filename: str, data: str) -> None:
  """Writes the given data to a Google Cloud Storage Bucket."""
  gcs_client = storage.Client()
  gcs_bucket = gcs_client.get_bucket(bucket_name)
  gcs_bucket.blob(os.path.join(path,
                               filename)).upload_from_string(data, 'text/csv')
