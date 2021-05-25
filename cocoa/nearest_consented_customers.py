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
"""Module to re-distribute conversion-values of non-consenting customers."""

# TODO(cahlheim): check scann package
import logging
from typing import Any, Callable, List, Sequence, Tuple, Union

import numpy as np
import pandas as pd
from scipy import sparse
from scipy import special
from sklearn import neighbors
from sklearn.metrics import pairwise


class NearestCustomerMatcher:
  """Class to find nearest neighbors and distribute conversion value.

  When we have a dataset of customers that gave consent to cookie-tracking, and
  customers that did not give consent, we want to ensure that the total
  conversion values (e.g. value of a purchase) across all customers are
  accessible to SmartBidding.
  The NearestCustomerMatcher finds the most similar customers among the
  consenting customers to each of the non-consenting customers, and distributes
  the conversion values of any non-consenting customer across the matches in the
  set of consenting customers, in proportion to their distance.
  Similarity is defined as the distance between customers in their feature-
  space, for instance based on adgroup-levels. Which distance-metric to
  choose is up to the user.
  The more similar a consenting customer is to a given non-consenting
  customer, the larger the share of the non-consenting customer's conversion-
  value that will be added to the consenting customer's conversion value.
  """

  def __init__(self,
               data_consent: pd.DataFrame,
               conversion_column: str,
               id_columns: List[Union[str, int]],
               metric: str = "manhattan",
               neighbor: Callable[..., Any] = neighbors.NearestNeighbors):
    """Initialises class.

    Args:
      data_consent: Dataframe of consented customers (preprocessed).
      conversion_column: Name of column in dataframe of conversion-value.
      id_columns: Names of columns that identify customers. Usually GCLID and
        timestamp.
      metric: Distance metric to use when finding nearest neighbors.
      neighbor: sklearn NearestNeighbor object.

    Raises:
      ValueError if the conversion values contain NaNs or Nones, or if
        conversion values < 0.
    """
    # TODO() Test behaviour under different distance metrics.
    self._neighbor = neighbor(metric=metric, algorithm="auto")
    self._columns_consent = data_consent.drop(id_columns, axis=1).columns
    self._data_consent = data_consent[id_columns + [conversion_column]]
    features_consent = data_consent.drop(id_columns + [conversion_column],
                                         axis=1).astype(np.float16).values
    self._features_consent = sparse.csr_matrix(features_consent)
    self._conversion_column = conversion_column
    self._consent_id = data_consent[id_columns]
    self._id_columns = id_columns
    if any(self._data_consent[self._conversion_column].isna()):
      raise ValueError("The conversion column must not contain NaNs/Nones.")
    if any(self._data_consent[self._conversion_column] <= 0):
      raise ValueError("The conversion values must be larger than zero.")
    self._neighbor = self._neighbor.fit(self._features_consent)

    # These attributes will be populated with data later.
    self._data_noconsent = None
    self._data_noconsent_match = None
    self._data_noconsent_nomatch = None

  @property
  def total_non_matched_conversion_value(self) -> float:
    return self._data_noconsent_nomatch[self._conversion_column].sum()

  @property
  def total_matched_conversion_value(self) -> float:
    return self._data_noconsent_match[self._conversion_column].sum()

  @property
  def percentage_matched_conversion_value(self) -> float:
    return (self.total_matched_conversion_value /
            (self.total_non_matched_conversion_value +
             self.total_matched_conversion_value)) * 100

  @property
  def number_non_matched_conversions(self) -> int:
    return len(self._data_noconsent_nomatch)

  @property
  def number_matched_conversions(self) -> int:
    return len(self._data_noconsent_match)

  @property
  def percentage_matched_conversions(self) -> float:
    return self.number_matched_conversions / len(self._data_noconsent) * 100

  @property
  def distance_statistics(self):
    return self._data_adjusted["average_distance"].describe()

  @property
  def nearest_distances_statistics_nonconsenting(self):
    return self._data_noconsent_match["distance_to_nearest_neighbor"].describe(
        percentiles=[.25, .5, .75, .9, .95, .99])

  @property
  def get_permuted_distances(self):
    permuted_distances = pairwise.paired_distances(self._data_examples,
                                                   self._data_permuted,
                                                   metric=self._neighbor.metric)
    return permuted_distances

  @property
  def summary_statistics_matched_conversions(self):
    return pd.DataFrame(
        {
            "percentage_matched_conversion_value":
                self.percentage_matched_conversion_value,
            "percentage_matched_conversions":
                self.percentage_matched_conversions,
            "number_matched_conversions":
                self.number_matched_conversions,
            "total_matched_conversion_value":
                self.total_matched_conversion_value
        },
        index=["summary_statistics_matched_conversions"])

  def min_radius_by_percentile(self, percentile: float = .95) -> float:
    radius = self._data_noconsent_match[
        "distance_to_nearest_neighbor"].quantile(percentile)
    return radius

  def _get_proportional_number_nearest_neighbors(
      self, number_nearest_neighbors: float) -> int:
    return int(number_nearest_neighbors * len(self._data_consent))

  def _fit_neighbor(self):
    self._neighbor.fit(self._features_consent)
    self._fitted = True

  def _get_neighbors_within_radius(
      self, data_noconsent: pd.DataFrame, radius: float
  ) -> Tuple[Sequence[np.ndarray], Sequence[np.ndarray], Sequence[bool]]:
    """Gets neighbors within specified radius.

    Args:
      data_noconsent: Data of non-consenting customers.
      radius: Radius within which nearest neighbors are found.

    Returns:
      neighbors_index: Array of indices-arrays of neighboring points.
      neighbors_distances: Array of distance-arrays to neighboring points.
      has_neighbors_array: Array of booleans indicating whether a given non-
        consenting customer had at least one neighbor or not. Takes advantage
        of numpy's functionality, e.g.:
        (np.array([0,1,2]) > 0)
        >>> array([False,  True,  True])
    """
    neighbors_distance, neighbors_index = self._neighbor.radius_neighbors(
        data_noconsent.drop([self._conversion_column], axis=1),
        radius=radius,
        return_distance=True,
    )
    has_neighbors_array = np.array(
        [len(neighbors) for neighbors in neighbors_index]) > 0
    if not any(has_neighbors_array):
      logging.warning("No matching customers within radius %d.", radius)
    neighbors_index = neighbors_index[has_neighbors_array]
    neighbors_distance = neighbors_distance[has_neighbors_array]
    return neighbors_index, neighbors_distance, has_neighbors_array

  def _get_n_nearest_neighbors(
      self, data_noconsent: pd.DataFrame, number_nearest_neighbors: float
  ) -> Tuple[Sequence[np.ndarray], Sequence[np.ndarray], Sequence[bool]]:
    """Gets n nearest neighbors.

    Args:
      data_noconsent: Data of non-consenting customers.
      number_nearest_neighbors: Number of neighbors to return. If <1,
        number_nearest_neighbors is calculated as the proportion in the set of
        consenting customers.

    Returns:
      neighbors_index: Array of indices-arrays of neighboring points.
      neighbors_distances: Array of distance-arrays to neighboring points.
      has_neighbors_array: Array of booleans indicating whether a given non-
        consenting customer had at least one neighbor or not. Takes advantage
        of numpy's functionality, e.g.:
        (np.array([0,1,2]) > 0)
        >>> array([False,  True,  True])

    Raises:
      ValueError if the actual number of nearest neighbors is not
        `number_nearest_neighbors`.
    """
    if number_nearest_neighbors < 1:
      number_nearest_neighbors = (
          self._get_proportional_number_nearest_neighbors(
              number_nearest_neighbors))
    neighbors_distance, neighbors_index = self._neighbor.kneighbors(
        data_noconsent.drop([self._conversion_column], axis=1),
        n_neighbors=number_nearest_neighbors,
        return_distance=True)
    has_neighbors_array = np.array(
        [len(neighbors) for neighbors in neighbors_index]) > 0
    if np.shape(neighbors_distance)[1] != number_nearest_neighbors:
      raise ValueError(
          f"Returned number of neighbors is not {number_nearest_neighbors}.")
    return neighbors_index, neighbors_distance, has_neighbors_array

  def _get_nearest_neighbors(
      self,
      data_noconsent: pd.DataFrame,
      radius: float = None,
      number_nearest_neighbors: float = None
  ) -> Tuple[Sequence[np.ndarray], Sequence[np.ndarray], Sequence[bool]]:
    """Get indices and distances to nearest neighbors.

    Finds nearest neighbors based on radius or number_nearest_neighbors for each
    entry in data_noconsent. If nearest neighbors are defined
    via radius, entries in data_noconsent without sufficiently close
    neighbor are removed.

    Args:
      data_noconsent: Data of non-consenting customers.
      radius: Radius within which neighbors have to lie.
      number_nearest_neighbors: Defines the number (or proportion) of nearest
        neighbors. If smaller 1, number_nearest_neighbors is calculated as the
        proportion of the number of consenting customers.

    Returns:
      A 3-tuple with:
        Array of indices-arrays of nearest neighbors in data_consent.
        Array of distances-arrays of nearest neigbors in data_consent.
        Array of booleans indicating whether a given non-consenting customer
        had at least one neighbor or not.

    Raises:
      ValueError if not exactly one of radius or number_nearest_neighbors are
        provided.
    """
    has_radius = radius is not None
    has_number_nearest_neighbors = number_nearest_neighbors is not None

    if has_radius == has_number_nearest_neighbors:
      raise ValueError("Exactly one of radius or number_nearest_neighbors has ",
                       "to be provided.")
    if has_radius:
      return self._get_neighbors_within_radius(data_noconsent, radius)
    elif has_number_nearest_neighbors:
      return self._get_n_nearest_neighbors(data_noconsent,
                                           number_nearest_neighbors)

  def _assert_all_columns_match_and_conversions_are_valid(self, data_noconsent):
    """Checks that all consenting and non-consenting data match and are valid.

    Args:
      data_noconsent: Data of non-consenting customers.

    Raises:
      ValueError if columns of consenting and non-consenting data don't match,
        the conversion values contain NaNs/Nones or if conversion values <0.
    """
    if not all(self._columns_consent == data_noconsent.columns):
      raise ValueError(
          "Consented and non-consented data must have same columns.")
    for data in (data_noconsent, self._data_consent):
      if any(data[self._conversion_column].isna()):
        raise ValueError("The conversion column should not contain NaNs.")
      if any(data[self._conversion_column] <= 0):
        ValueError("The conversion values should be larger than zero.")

  def get_indices_and_values_to_nearest_neighbors(
      self,
      data_noconsent: pd.DataFrame,
      radius: float = None,
      number_nearest_neighbors: float = None
  ) -> Tuple[Sequence[np.ndarray], Sequence[np.ndarray], Sequence[np.ndarray],
             Sequence[np.ndarray], Sequence[bool]]:
    """Gets indices of nearest neighbours as well as the needed conversions.

    Args:
      data_noconsent: Data of non-consenting customers.
      radius: Radius within which neighbors have to lie.
      number_nearest_neighbors: Defines the number (or proportion) of nearest
        neighbors.

    Returns:
      neighbors_data_index: Arrays of indices to the nearest neighbors in the
        consenting-customer data.
      neighbors_distance: Arrays of distances to the nearest neighbors.
      weighted_conversion_values: Conversion values of non-consenting customers
        weighted by their distance to each nearest neighbor.
      weighted_distance: Weighted distances between non-consenting and
        consenting customers.
      has_neighbor: Whether or not a given non-consenting customer had a
        nearest neighbor.
    """

    data_noconsent = data_noconsent.drop(self._id_columns, axis=1)
    self._assert_all_columns_match_and_conversions_are_valid(data_noconsent)
    neighbors_index, neighbors_distance, has_neighbor = (
        self._get_nearest_neighbors(data_noconsent, radius,
                                    number_nearest_neighbors))
    neighbors_data_index = [
        self._data_consent.index[index] for index in neighbors_index
    ]
    non_consent_conversion_values = data_noconsent[has_neighbor][
        self._conversion_column].values
    weighted_conversion_values, weighted_distance = (
        _calculate_weighted_conversion_values(
            non_consent_conversion_values,
            neighbors_distance,
        ))
    return (neighbors_data_index, neighbors_distance,
            weighted_conversion_values, weighted_distance, has_neighbor)

  def calculate_adjusted_conversions(
      self,
      data_noconsent: pd.DataFrame,
      radius: float = None,
      number_nearest_neighbors: float = None) -> pd.DataFrame:
    """Calculates adjusted conversions for identified nearest neighbors.

    Finds nearest neighbors based on radius or number_nearest_neighbors for each
    entry in data_noconsent. If nearest neighbors are defined via radius,
    entries in data_noconsent without sufficiently close neighbor are ignored.
    Conversion values of consenting customers that are identified as nearest
    neighbor to a non-consenting customer are adjusted by adding the weighted
    proportional conversion value of the respective non-consenting customer.
    The weighted conversion value is calculated as the product of the conversion
    value with the softmax over all neighbor-similarities.

    Args:
      data_noconsent: Data for non-consenting customer(s). Needs to be pre-
        processed and have the same columns as data_consent.
      radius: Radius within which neighbors have to lie.
      number_nearest_neighbors: Defines the number (or proportion) of nearest
        neighbors.

    Returns:
      data_adjusted: Copy of data_consent including the modelled conversion
        values.
    """
    (neighbors_data_index, neighbors_distance, weighted_conversion_values,
     weighted_distance,
     has_neighbor) = self.get_indices_and_values_to_nearest_neighbors(
         data_noconsent, radius, number_nearest_neighbors)
    self._data_noconsent = data_noconsent.drop(self._id_columns, axis=1)
    self._data_noconsent_nomatch = data_noconsent[np.invert(
        has_neighbor)].copy()
    self._data_noconsent_match = data_noconsent[has_neighbor].copy()
    self._data_noconsent_match["distance_to_nearest_neighbor"] = [
        min(distances) for distances in neighbors_distance
    ]
    self._data_adjusted = _distribute_conversion_values(
        self._data_consent, self._conversion_column,
        self._data_noconsent_match[self._conversion_column].values,
        weighted_conversion_values, neighbors_data_index, neighbors_distance,
        weighted_distance)
    return self._data_adjusted

  def create_self_permutation_and_return_distances(
      self,
      n_examples: int,
      level: str = "5",
      recursive: bool = True,
  ) -> Sequence[float]:
    """Calculate distance to permuted-self for n samples.

    For n random samples,set the adgroup variables at levels from 'level'
    onwards to 0 and return the resulting distance to the original datapoint.
    This serves as indication of the robustness of the nearest-neighbor model.

    Args:
     n_examples: Number of examples.
     level: Level of adgroup variables that should be changed.
     recursive: If True, changes all lower adgroup levels, too.

    Returns:
      permuted_distances: Array of n_examples distances under permutation.
    """

    data_examples = self._data_noconsent.sample(n_examples,
                                                random_state=42).copy()
    data_examples.drop([self._conversion_column], axis=1, inplace=True)

    adgroup_array = self._data_noconsent.columns[
        self._data_noconsent.columns.str.startswith("adgroup_level")]
    level_array = np.array([col.split("_")[3] for col in adgroup_array])
    if adgroup_array.empty or len(level_array) == 0:
      raise ValueError(
          "No adgroup or adgroup-levels could be found.",
          "Please ensure all adgroup-columns begin with 'adgroup'.")
    data_permuted = data_examples.copy()
    data_permuted = _set_adgroup_by_level_to_zero(data_permuted,
                                                  adgroup_array,
                                                  level_array,
                                                  level=level,
                                                  recursive=recursive)
    data_permuted.sort_index(inplace=True)
    data_examples.sort_index(inplace=True)

    permuted_distances = pairwise.paired_distances(data_examples,
                                                   data_permuted,
                                                   metric=self._neighbor.metric)
    self._data_permuted = data_permuted
    self._data_examples = data_examples
    return permuted_distances


def _calculate_weighted_conversion_values(
    conversion_values: Sequence[np.ndarray],
    neighbors_distance: Sequence[np.ndarray],
) -> Tuple[Sequence[np.float], Sequence[np.float]]:
  """Calculate weighted conversion values as function of distance.

  The weighted conversion value is calculated as the product of the conversion
  value with the softmax over all neighbor-similarities.


  Args:
    conversion_values: Array of conversion_values for non-consented customers.
    neighbors_distance: Array of arrays of neighbor-distances.

  Returns:
    weighted_conversion_values: Array of weighted conversion_values per non-
     consented customer.
    softmax_similarity: Array of softmax similarities per non-consented
     customer.
  """
  if len(conversion_values) != len(neighbors_distance):
    raise ValueError("All of conversion_values and neighbors_distance",
                     "must have the same length.")

  if any((dist < 0).any() for dist in neighbors_distance):
    raise ValueError("Distances should not contain negative values."
                     "Please review which distance metric you used.")

  softmax_similarity = [
      special.softmax(-distance) for distance in neighbors_distance
  ]
  weighted_conversion_values = [
      conversion_value * weight
      for conversion_value, weight in zip(conversion_values, softmax_similarity)
  ]
  return weighted_conversion_values, softmax_similarity


def _distribute_conversion_values(
    data_consent: pd.DataFrame,
    conversion_column: str,
    non_consent_conversion_values: Sequence[float],
    weighted_conversion_values: Sequence[np.ndarray],
    neighbors_index: Sequence[np.ndarray],
    neighbors_distance: Sequence[np.ndarray],
    weighted_distance: Sequence[np.ndarray],
) -> pd.DataFrame:
  """Distribute conversion-values of non-consenting over consenting customers.

  Conversion values of consenting customers that are identified as nearest
  neighbor to a non-consenting customer are adjusted by adding the weighted
  proportional conversion value of the respective non-consenting customer.
  Additionally, metrics like average distance to non-consenting customers
  and total number of added conversions are calculated.

  Args:
    data_consent: DataFrame of consented customers.
    conversion_column: String indicating the conversion KPI in data_consent.
    non_consent_conversion_values: Array of original conversion values.
    weighted_conversion_values: Array of arrays of weighted conversion_values,
      based on distance between consenting and non-consenting customers.
    neighbors_index: Array of arrays of neighbor-indices.
    neighbors_distance: Array of arrays of neighbor-distances.
    weighted_distance: Array of arrays of weighted neighbor-distances.

  Returns:
    data_adjusted: Copy of data_consent including the modelled conversion
      values.
  """

  data_adjusted = data_consent.copy()
  data_adjusted["adjusted_conversion"] = 0
  data_adjusted["average_distance"] = 0
  data_adjusted["n_added_conversions"] = 0
  data_adjusted["sum_distribution_weights"] = 0
  for index, values, distance, weight in zip(neighbors_index,
                                             weighted_conversion_values,
                                             neighbors_distance,
                                             weighted_distance):
    data_adjusted.loc[index, "adjusted_conversion"] += values
    data_adjusted.loc[index, "average_distance"] += distance
    data_adjusted.loc[index, "sum_distribution_weights"] += weight
    data_adjusted.loc[index, "n_added_conversions"] += 1

  data_adjusted["average_distance"] = (data_adjusted["average_distance"] /
                                       data_adjusted["n_added_conversions"])

  naive_conversion_adjustments = np.sum(non_consent_conversion_values) / len(
      data_consent)
  data_adjusted["naive_adjusted_conversion"] = data_adjusted[
      conversion_column] + naive_conversion_adjustments
  return data_adjusted


def _set_adgroup_by_level_to_zero(data: pd.DataFrame,
                                  adgroup_array: Sequence[Any],
                                  level_array: Sequence[str],
                                  level: str = "5",
                                  recursive: bool = True) -> pd.DataFrame:
  """Set dummy-variables for ad-group to zero for given level.

  For each adgroup corresponding to 'level', sets all to 0. Effectively, this
  removes the adgroup level from the ad-hierarchy.

  Args:
     data: DataFrame for which adgroup-fields should be permuted.
     adgroup_array: List of column names of adgroup features in dataframe.
     level_array: List of levels for each adgroup name. Has to match
       adgroup_array.
     level: Level of adgroup variables that should be changed.
     recursive: If True, changes all lower adgroup levels, too.

  Returns:
    data_permuted: Dataframe with permuted adgroup-columns.
  """
  level_columns = adgroup_array[level_array == level]
  data.loc[:, level_columns] = 0
  if recursive:
    next_level = str(int(level) + 1)
    if next_level <= max(level_array):
      _set_adgroup_by_level_to_zero(data=data,
                                    adgroup_array=adgroup_array,
                                    level_array=level_array,
                                    level=next_level,
                                    recursive=True)
  return data
