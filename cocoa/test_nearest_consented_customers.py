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

"""Tests for nearest_consented_customers package."""

import numpy as np

from consent_based_conversion_adjustments.cocoa import nearest_consented_customers
from consent_based_conversion_adjustments.cocoa import testing_constants
from absl.testing import absltest
from absl.testing import parameterized

CONVERSION_COLUMN = testing_constants.CONVERSION_COLUMN
ID_COLUMNS = testing_constants.ID_COLUMNS

DATA_CONSENT = testing_constants.DATA_CONSENT
DATA_NOCONSENT = testing_constants.DATA_NOCONSENT

METRIC = testing_constants.METRIC


class NearestCustomerTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.matcher = nearest_consented_customers.NearestCustomerMatcher(
        DATA_CONSENT, CONVERSION_COLUMN, ID_COLUMNS, METRIC)
    self.matcher._data_noconsent = DATA_NOCONSENT.drop(ID_COLUMNS, axis=1)

  def test_columns_differ_raises_value_error(self):
    with self.assertRaises(ValueError):
      self.matcher.calculate_adjusted_conversions(
          data_noconsent=DATA_NOCONSENT.drop('a', axis=1),
          number_nearest_neighbors=1)

  def test_number_nearest_neighbors_and_radius_raises_value_error(self):
    with self.assertRaises(ValueError):
      self.matcher.calculate_adjusted_conversions(data_noconsent=DATA_NOCONSENT,
                                                  radius=.5,
                                                  number_nearest_neighbors=5)

  @parameterized.parameters(1, 2, 3)
  def test_assert_k_matches_length_of_indices(self, number_nearest_neighbors):
    indices, distances, _ = self.matcher._get_nearest_neighbors(
        data_noconsent=DATA_CONSENT.drop(ID_COLUMNS, axis=1),
        number_nearest_neighbors=number_nearest_neighbors)

    self.assertEqual(np.shape(indices)[1], number_nearest_neighbors)
    self.assertEqual(np.shape(distances)[1], number_nearest_neighbors)

  def test_number_nearest_neighbors_larger_than_customers_raises_value_error(
      self):
    number_nearest_neighbors = len(DATA_CONSENT) * 3

    with self.assertRaises(ValueError):
      _ = self.matcher.calculate_adjusted_conversions(
          data_noconsent=DATA_NOCONSENT,
          number_nearest_neighbors=number_nearest_neighbors)

  def test_no_match_in_radius_logs_warning(self):
    with self.assertLogs(level='WARNING') as log:
      _ = self.matcher.calculate_adjusted_conversions(
          data_noconsent=DATA_NOCONSENT, radius=0)

      self.assertEqual(log.records[0].getMessage(),
                       'No matching customers within radius 0.')

  def test_adjusted_conversion_value_different_from_original_value(self):
    data_adjusted = self.matcher.calculate_adjusted_conversions(
        data_noconsent=DATA_NOCONSENT, number_nearest_neighbors=3)

    sum_adjusted_conversions = (data_adjusted[CONVERSION_COLUMN].sum()
                                + data_adjusted['adjusted_conversion'].sum())
    self.assertLess(data_adjusted[CONVERSION_COLUMN].sum(),
                    sum_adjusted_conversions)

  def test_negative_distances_raises_value_error(self):
    neighbors_distance = [np.array([0.1, 0.1, 0.8]), np.array([-5, 0, 0])]

    with self.assertRaises(ValueError):
      nearest_consented_customers._calculate_weighted_conversion_values(
          DATA_NOCONSENT[CONVERSION_COLUMN].values, neighbors_distance)

  def test_neighbor_indices_not_in_data_consent_raises_key_error(self):
    neighbors_index = [np.array([111, 222, 333])]
    neighbors_distance = [np.array([0.1, 0.1, 0.8])]
    weighted_conversion_values = [np.array([10, 20, 30])]
    weighted_distances = neighbors_distance

    with self.assertRaises(KeyError):
      nearest_consented_customers._distribute_conversion_values(
          DATA_CONSENT, CONVERSION_COLUMN,
          DATA_CONSENT[CONVERSION_COLUMN].values, weighted_conversion_values,
          neighbors_index, neighbors_distance, weighted_distances)

  def test_adjusted_conversion_smaller_than_upper_limit(self):
    upper_limit = DATA_NOCONSENT[CONVERSION_COLUMN].sum()

    data_adjusted = self.matcher.calculate_adjusted_conversions(
        data_noconsent=DATA_NOCONSENT, radius=1)
    sum_adjusted_conversions = data_adjusted['adjusted_conversion'].sum()

    self.assertLessEqual(sum_adjusted_conversions, upper_limit)

  def test_sum_of_weighted_conversions_matches_original_conversions(self):
    original_conversions = DATA_NOCONSENT[CONVERSION_COLUMN].astype(
        float).values[:2]
    neighbors_distance = [np.array([0.1, 0.1, 0.8]), np.array([1, 0, 0])]

    weighted_conversions, _ = (
        nearest_consented_customers._calculate_weighted_conversion_values(
            original_conversions, neighbors_distance))

    np.testing.assert_almost_equal(np.sum(weighted_conversions, axis=1),
                                   original_conversions)

  def test_permuted_distances_larger_zero(self):
    permuted_dist = self.matcher.create_self_permutation_and_return_distances(
        n_examples=1, level='1')

    self.assertLess(0, permuted_dist.mean())

  # TODO() Add test to assert expected outcome is produced.


if __name__ == '__main__':
  absltest.main()
