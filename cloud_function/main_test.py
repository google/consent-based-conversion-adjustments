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

"""Tests for main."""
import datetime

from absl.testing import parameterized
from consent_based_conversion_adjustments.cloud_function import main


class PipelineTest(parameterized.TestCase):

  @parameterized.named_parameters(
      dict(
          testcase_name='_start_date_as_single_entry_list',
          start_date='2021-12-15',
          lookback_window=1,
          expected_output=[datetime.date.fromisoformat('2021-12-15')]),
      dict(
          testcase_name='_list_with_start_date_and_previous_day',
          start_date='2021-12-15',
          lookback_window=2,
          expected_output=[
              datetime.date.fromisoformat('2021-12-15'),
              datetime.date.fromisoformat('2021-12-14')
          ]))
  def test_get_dates_to_process_returns(self, start_date, lookback_window,
                                        expected_output):
    result = main.get_dates_to_process(start_date, lookback_window)
    self.assertListEqual(result, expected_output)
