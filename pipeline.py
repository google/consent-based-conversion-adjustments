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

"""Pipeline to run conversion adjustments.

Consenting and non-consenting data for customers is read by the pipeline and
adjustments are applied to conversion values of consenting
customers based on the distance from non-consenting customers and the runtime
arguments like number_nearest_neighbors, radius and percentile. The adjusted
data is output as a csv where the adjusted conversions appear in a new column.
"""
import argparse
import datetime
import logging
import os
import sys
from typing import Any, Callable, List, Optional, Sequence, Tuple, Union

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.value_provider import RuntimeValueProvider
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd

from consent_based_conversion_adjustments.cocoa import nearest_consented_customers
from consent_based_conversion_adjustments.cocoa import preprocess

logging.basicConfig(level=logging.INFO)


def _parse_known_args(
    cmd_line_args: Sequence[str]) -> Tuple[argparse.Namespace, Sequence[str]]:
  """Parses known arguments from the command line using the argparse library.

  Args:
    cmd_line_args: Sequence of commandline arguments.

  Returns:
    A tuple containing argparse.Namespace with known arguments and a list of
    remaining (unknown) command line arguments.
  """
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input_path',
      dest='input_path',
      required=True,
      help='Path to txt file containing dates for which to run the pipeline.')
  parser.add_argument(
      '--output_csv_bucket',
      dest='output_csv_bucket',
      required=True,
      help='Google Cloud Storage bucket for storing CSV output.')
  parser.add_argument(
      '--output_csv_path',
      dest='output_csv_path',
      required=True,
      help='CSV output file location.')
  parser.add_argument(
      '--bq_project',
      dest='bq_project',
      required=True,
      help='Google Cloud project containing the BigQuery tables.')
  parser.add_argument(
      '--location',
      dest='location',
      required=True,
      help='Location of the BigQuery tables e.g EU')
  parser.add_argument(
      '--table_consent',
      dest='table_consent',
      required=True,
      help='BigQuery table containing consented user data.')
  parser.add_argument(
      '--table_noconsent',
      dest='table_noconsent',
      required=True,
      help='BigQuery table containing non-consented user data.')
  parser.add_argument(
      '--date_column',
      dest='date_column',
      required=True,
      help='BigQuery table column containing date value.')
  parser.add_argument(
      '--conversion_column',
      dest='conversion_column',
      required=True,
      help='BigQuery table column containing conversion value.')
  parser.add_argument(
      '--id_columns',
      dest='id_columns',
      required=True,
      help='BigQuery table columns that form a unique row e.g. GCLID,TIMESTAMP.'
  )
  parser.add_argument(
      '--drop_columns',
      dest='drop_columns',
      required=False,
      help='BigQuery table columns that should be dropped from the data.')
  parser.add_argument(
      '--non_dummy_columns',
      dest='non_dummy_columns',
      required=False,
      help='BigQuery table (categorical) columns that should be kept, but not dummy-coded.'
  )
  return parser.parse_known_args(cmd_line_args)


class RuntimeOptions(PipelineOptions):
  """Specifies runtime options for the pipeline.

  Class defining the arguments that can be passed to the pipeline to
  customize the runtime execution.
  """

  @classmethod  # classmethod is required here for Beam's PipelineOptions.
  def _add_argparse_args(cls, parser):
    parser.add_value_provider_argument(
        '--number_nearest_neighbors',
        help='number of nearest consenting customers to select.')
    parser.add_value_provider_argument(
        '--radius',
        help='radius within which nearest customers should be considered.')
    parser.add_value_provider_argument(
        '--percentile',
        help='percentage of non-consenting customers that should be matched.')
    parser.add_value_provider_argument(
        '--metric', help='distance metric.', type=str)


def _load_data_from_bq(table_name: str, location: str, project: str,
                       start_date: str, end_date: str,
                       date_column: str) -> pd.DataFrame:
  """Reads data from BigQuery filtered to the given start and end date."""
  bq_client = bigquery.Client(location=location, project=project)
  query = f"""
           SELECT * FROM `{table_name}`
           WHERE {date_column} >= '{start_date}' and {date_column} < '{end_date}'
           ORDER BY {date_column}
           """
  return bq_client.query(query).result().to_dataframe()


class ConversionAdjustments(beam.DoFn):
  """Apache Beam ParDo transform for applying conversion adjustments."""

  def __init__(self, number_nearest_neighbors: RuntimeValueProvider,
               radius: RuntimeValueProvider, percentile: RuntimeValueProvider,
               metric: RuntimeValueProvider, project: str, location: str,
               table_consent: str, table_noconsent: str, date_column: str,
               conversion_column: str, id_columns: List[str],
               drop_columns: Tuple[Any,
                                   ...], non_dummy_columns: Tuple[Any,
                                                                  ...]) -> None:
    """Initialises class.

    Args:
      number_nearest_neighbors: Number of nearest consenting customers to
        select.
      radius: Radius within which nearest customers should be considered.
      percentile: Percentage of non-consenting customers that should be matched.
      metric: Distance metric e.g. manhattan.
      project: Name of Google Cloud project containing the BigQuery tables.
      location: Location of the BigQuery tables e.g. EU.
      table_consent: BigQuery table containing consented user data.
      table_noconsent: BigQuery table containing non-consented user data.
      date_column: BigQuery table column containing date value.
      conversion_column: BigQuery table column containing conversion value.
      id_columns: BigQuery table columns that form a unique row.
      drop_columns: BigQuery table columns that should be dropped from the data.
      non_dummy_columns: BigQuery table (categorical) columns that should be
        kept, but not dummy-coded.
    """
    self._number_nearest_neighbors = number_nearest_neighbors
    self._radius = radius
    self._percentile = percentile
    self._metric = metric
    self._project = project
    self._location = location
    self._table_consent = table_consent
    self._table_noconsent = table_noconsent
    self._date_column = date_column
    self._conversion_column = conversion_column
    self._id_columns = id_columns
    self._drop_columns = drop_columns
    self._non_dummy_columns = non_dummy_columns

  def process(
      self, process_date: datetime.date
  ) -> Optional[Sequence[Tuple[str, pd.DataFrame, pd.DataFrame]]]:
    """Calculates conversion adjustments for the given date.

    Args:
      process_date: Date to be processed.

    Returns:
      Tuple containing processed date, adjusted data and summary statistics.
    """
    logging.info('Processing date %r', process_date)
    # TODO(): Consider if time delta can be decided by user.
    end_date = str((process_date + datetime.timedelta(days=1)))
    start_date = str(process_date)
    logging.info('Pulling non-consented data for date %r', process_date)
    data_noconsent = _load_data_from_bq(self._table_noconsent, self._location,
                                        self._project, start_date, end_date,
                                        self._date_column)
    logging.info('Pulling consented data for date %r', process_date)
    data_consent = _load_data_from_bq(self._table_consent, self._location,
                                      self._project, start_date, end_date,
                                      self._date_column)
    logging.info(
        'Preprocessing consented and non-consented datasets for date %r',
        process_date)
    data_consent, data_noconsent = preprocess.concatenate_and_process_data(
        data_consent, data_noconsent, self._conversion_column,
        self._drop_columns, self._non_dummy_columns)
    matcher = nearest_consented_customers.NearestCustomerMatcher(
        data_consent, self._conversion_column, self._id_columns,
        _get_runtime_val_or_none(self._metric))
    logging.info('Calculating conversion adjustments for date %r', process_date)

    data_adjusted, summary_statistics_matched_conversions = nearest_consented_customers.get_adjustments_and_summary_calculations(
        matcher, data_noconsent,
        _get_runtime_val_or_none(self._number_nearest_neighbors, int),
        _get_runtime_val_or_none(self._radius, float),
        _get_runtime_val_or_none(self._percentile, float))
    return [(start_date, data_adjusted, summary_statistics_matched_conversions)]


def _get_runtime_val_or_none(
    runtime_var: RuntimeValueProvider,
    apply_type: Callable[[Union[int, float, str]], Union[int, float, str]] = str
) -> Optional[Union[int, float, str]]:
  """Gets the runtime value in the correct type.

  Checks if a runtime value is available. If the value is not None, convert the
  value to the requested type.

  Args:
    runtime_var: The runtime value provider.
    apply_type: A type that may be applied to non-none runtime values.

  Returns:
    Typed value if available, otherwise None.
  """
  if runtime_var.is_accessible():
    runtime_val = runtime_var.get()
    if runtime_val is not None:
      return apply_type(runtime_val)
  return None


def write_adjustments_to_gcs(adjustments: Tuple[str, pd.DataFrame,
                                                pd.DataFrame], bucket_name: str,
                             path: str) -> None:
  """Prepares the conversion adjustments data to be written to Cloud Storage.

  Args:
    adjustments: A tuple containing processed date, adjusted data and summary
      statistics.
    bucket_name: Name of the Cloud Storage bucket where adjustments are written.
    path: Path on the Cloud Storage bucket where adjustments are written.

  Returns:
    None.
  """
  adjustments_date = adjustments[0]
  adjustments_data = adjustments[1].to_csv(index=False)
  adjustments_summary = adjustments[2].to_csv(index=False)
  gcs_client = storage.Client()
  gcs_bucket = gcs_client.get_bucket(bucket_name)
  logging.info('Uploading conversion adjustments for date %r', adjustments_date)
  write_to_gcs(gcs_bucket, os.path.join(path, adjustments_date),
               'adjustments_data.csv', 'text/csv', adjustments_data)
  logging.info('Uploading adjustments summary for date %r', adjustments_date)
  write_to_gcs(gcs_bucket, os.path.join(path, adjustments_date),
               'adjustments_summary.csv', 'text/csv', adjustments_summary)


def write_to_gcs(bucket: storage.Bucket, path: str, filename: str,
                 data_type: str, data: str) -> None:
  """Writes data to the given Cloud Storage bucket."""
  bucket.blob(os.path.join(path, filename)).upload_from_string(data, data_type)


def get_columns_from_str(columns: Optional[str],
                         separator: str = ',') -> Tuple[Any, ...]:
  """Converts columns input as separated string to tuples for further processing.

  A helper function to convert strings containing column names to tuples of
  column names.

  Args:
    columns: List of columns as a string with separators.
    separator: Character that separates the column names in the string.

  Returns:
    A tuple containing the columns names or empty if the column string doesn't
      exist or is empty.
  """
  if not columns:
    return ()
  return tuple(columns.split(separator))


def main(argv: Sequence[str], save_main_session: bool = True) -> None:
  """Main entry point; defines and runs the beam pipeline."""

  known_args, pipeline_args = _parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  runtime_options = pipeline_options.view_as(RuntimeOptions)

  with beam.Pipeline(options=pipeline_options) as p:

    dates_to_process = (
        p
        | 'Read ISO format date string from input file' >> beam.io.ReadFromText(
            known_args.input_path)
        | 'Convert to date type' >> beam.Map(datetime.date.fromisoformat))

    adjustments = (
        dates_to_process
        | 'Apply conversion adjustments' >> beam.ParDo(
            ConversionAdjustments(
                number_nearest_neighbors=runtime_options
                .number_nearest_neighbors,
                radius=runtime_options.radius,
                percentile=runtime_options.percentile,
                metric=runtime_options.metric,
                project=known_args.bq_project,
                location=known_args.location,
                table_consent=known_args.table_consent,
                table_noconsent=known_args.table_noconsent,
                conversion_column=known_args.conversion_column,
                id_columns=list(known_args.id_columns.split(',')),
                date_column=known_args.date_column,
                drop_columns=get_columns_from_str(known_args.drop_columns),
                non_dummy_columns=get_columns_from_str(
                    known_args.non_dummy_columns))))

    _ = (
        adjustments
        | 'Write adjusted data as CSV files to cloud storage' >> beam.Map(
            write_adjustments_to_gcs,
            bucket_name=known_args.output_csv_bucket,
            path=known_args.output_csv_path))


if __name__ == '__main__':
  main(sys.argv)
