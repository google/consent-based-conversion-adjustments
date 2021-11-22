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

"""Tests for preprocess."""

from absl.testing import absltest
from absl.testing import parameterized
import pandas as pd


from consent_based_conversion_adjustments.cocoa import preprocess
from consent_based_conversion_adjustments.cocoa import testing_constants

CONVERSION_COLUMN = testing_constants.CONVERSION_COLUMN
ID_COLUMNS = testing_constants.ID_COLUMNS

DATA_CONSENT = testing_constants.DATA_CONSENT
DATA_NOCONSENT = testing_constants.DATA_NOCONSENT

METRIC = testing_constants.METRIC


class PreprocessTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.fake_data_consent = DATA_CONSENT
    self.fake_data_consent['new_customer'] = 0
    self.fake_data_consent.iloc[::3]['new_customer'] = 1
    self.fake_data_noconsent = DATA_NOCONSENT
    self.fake_data_noconsent['new_customer'] = 0
    self.fake_data_noconsent.iloc[::3]['new_customer'] = 1

  def test_processed_shape_matches_expected_shape(self):
    joined_data = pd.concat([self.fake_data_consent, self.fake_data_noconsent])
    categorical_columns = joined_data.columns[joined_data.dtypes == 'object']
    n_dummy_variables = 0
    for categorical_column in categorical_columns:
      n_dummy_variables += joined_data[categorical_column].nunique() -1
    target_shape = self.fake_data_consent.shape[1] + n_dummy_variables + 1

    fake_preprocessed_data_consent, fake_preprocessed_data_noconsent = (
        preprocess.concatenate_and_process_data(
            self.fake_data_consent.copy(), self.fake_data_noconsent.copy()))

    self.assertEqual(fake_preprocessed_data_consent.shape[1], target_shape)
    self.assertEqual(fake_preprocessed_data_noconsent.shape[1], target_shape)

  def test_preprocessed_conversion_values_larger_zero(self):
    fake_preprocessed_data_consent, fake_preprocessed_data_noconsent = (
        preprocess.concatenate_and_process_data(
            self.fake_data_consent.copy(), self.fake_data_noconsent.copy()))

    values_not_larger_zero = (
        fake_preprocessed_data_consent[CONVERSION_COLUMN] <= 0).sum() + (
            fake_preprocessed_data_noconsent[CONVERSION_COLUMN] <= 0).sum()

    self.assertEqual(values_not_larger_zero, 0)


if __name__ == '__main__':
  absltest.main()
