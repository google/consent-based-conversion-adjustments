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

"""Tests for pipeline."""
import dataclasses
import datetime

from absl.testing import absltest
from absl.testing import parameterized
import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import pandas as pd

from consent_based_conversion_adjustments import pipeline

_DATA_NOCONSENT = [{
    'gclid': '21',
    'conversion_timestamp': '2021-11-20 12:34:56 UTC',
    'conversion_value': 20.0,
    'conversion_date': '2021-11-20',
    'conversion_item': 'dress'
}]
_DATA_CONSENT = [{
    'gclid': '1',
    'conversion_timestamp': '2021-11-20 12:34:56 UTC',
    'conversion_value': 10.0,
    'conversion_date': '2021-11-20',
    'conversion_item': 'dress'
}]
_DATA_CONSENT_MULTI = [
    {
        'gclid': '1',
        'conversion_timestamp': '2021-11-20 12:34:56 UTC',
        'conversion_value': 10.0,
        'conversion_date': '2021-11-20',
        'conversion_item': 'dress'
    },
    {
        'gclid': '2',
        'conversion_timestamp': '2021-11-20 12:34:56 UTC',
        'conversion_value': 10.0,
        'conversion_date': '2021-11-20',
        'conversion_item': 'dress'
    },
]
_PIPELINE_RUN_DATE = '2021-11-20'
_PROJECT = 'cocoa_test'
_LOCATION = 'EU'
_TABLE_CONSENT = 'table_consent'
_TABLE_NOCONSENT = 'table_noconsent'
_CONVERSION_COLUMN = 'conversion_value'
_ID_COLUMNS = ['gclid', 'conversion_timestamp']
_DATE_COLUMN = 'conversion_date'
_DROP_COLUMNS = []
_NON_DUMMY_COLUMNS = _ID_COLUMNS


def _fake_load_data_from_bq(table_name: str, *args, **kwargs) -> pd.DataFrame:  # Patching method so pylint: disable=unused-argument
  if table_name == 'table_consent':
    return pd.DataFrame.from_records(_DATA_CONSENT)
  elif table_name == 'table_consent_multi':
    return pd.DataFrame.from_records(_DATA_CONSENT_MULTI)
  elif table_name == 'table_noconsent':
    return pd.DataFrame.from_records(_DATA_NOCONSENT)
  else:
    return pd.DataFrame([])


@dataclasses.dataclass(frozen=True)
class RuntimeParam:
  value: any
  is_accessible: bool = True

  def get(self):
    return self.value


class PipelineTest(parameterized.TestCase):

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    # Replace network calls to BigQuery with a local fake reply
    pipeline._load_data_from_bq = _fake_load_data_from_bq

  @parameterized.named_parameters(
      dict(
          testcase_name='_start_date_as_single_entry_list',
          start_date=RuntimeParam('2021-12-15'),
          lookback_window=RuntimeParam(1),
          expected_output=[datetime.date.fromisoformat('2021-12-15')]),
      dict(
          testcase_name='_list_with_start_date_and_previous_day',
          start_date=RuntimeParam('2021-12-15'),
          lookback_window=RuntimeParam(2),
          expected_output=[
              datetime.date.fromisoformat('2021-12-15'),
              datetime.date.fromisoformat('2021-12-14')
          ]))
  def test_get_dates_to_process_returns(self, start_date, lookback_window,
                                        expected_output):
    result = pipeline.get_dates_to_process(start_date, lookback_window)
    self.assertListEqual(result, expected_output)

  @parameterized.named_parameters(
      dict(
          testcase_name='_completely_when_single_nearest_neighbor',
          number_nearest_neighbors=1,
          table_consent='table_consent',
          expected_output=20.0),
      dict(
          testcase_name='_partially_when_multiple_nearest_neighbor',
          number_nearest_neighbors=2,
          table_consent='table_consent_multi',
          expected_output=10.0))
  def test_conversion_adjustments_value_assigned(self,
                                                 number_nearest_neighbors: int,
                                                 table_consent: str,
                                                 expected_output: float):

    with beam.Pipeline(beam.runners.direct.DirectRunner()) as p:
      date_to_process = (
          p | 'Process date' >> beam.Create(
              [datetime.date.fromisoformat(_PIPELINE_RUN_DATE)]))
      adjustments = (
          date_to_process
          | beam.ParDo(
              pipeline.ConversionAdjustments(
                  number_nearest_neighbors=RuntimeParam(
                      number_nearest_neighbors),
                  radius=RuntimeParam(None),
                  percentile=RuntimeParam(None),
                  metric=RuntimeParam('manhattan'),
                  project=_PROJECT,
                  location=_LOCATION,
                  table_consent=table_consent,
                  table_noconsent=_TABLE_NOCONSENT,
                  conversion_column=_CONVERSION_COLUMN,
                  id_columns=_ID_COLUMNS,
                  date_column=_DATE_COLUMN,
                  drop_columns=_DROP_COLUMNS,
                  non_dummy_columns=_NON_DUMMY_COLUMNS)))

      adjusted_conversion_value = (
          adjustments
          |
          'Select single row' >> beam.Map(lambda x: x[1][x[1]['gclid'] == '1'])
          | 'Calculate conversion value' >>
          beam.Map(lambda x: x['adjusted_conversion'].sum()))
      assert_that(adjusted_conversion_value, equal_to([expected_output]))


if __name__ == '__main__':
  absltest.main()
