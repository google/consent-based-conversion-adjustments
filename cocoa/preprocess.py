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

"""Prepare data to distribute conversion-values of no-consent customers."""

from typing import Any, List, Tuple

from absl import logging
import numpy as np
import pandas as pd

logging.set_verbosity(logging.WARNING)

NON_DUMMY_COLUMNS = ()  # typically at least "GCLID", "TIMESTAMP"
DROP_COLUMNS = ()
CONVERSION_COLUMN = "conversion_column"


def _clean_data(data: pd.DataFrame, conversion_column: str) -> pd.DataFrame:
  """Cleans data from NaNs and invalid conversion values.

  In its most basic form, this function drops entries that don't have a
  conversion value or for which the conversion value is not larger than zero.
  This function should be extended based on custom requirements.

  Args:
    data: Dataframe of customer data.
    conversion_column: Name of the conversion-column in data.

  Returns:
    cleaned data.
  """
  # Optional: Fill NaNs based on additional information/other columns.
  data.dropna(subset=[conversion_column], inplace=True)
  has_valid_conversion_value = data[conversion_column].values > 0
  data = data[has_valid_conversion_value]
  # Optional: Deduplicate consented users based on timestamp and gclid.
  return data


def _additional_feature_engineering(data: pd.DataFrame) -> pd.DataFrame:
  """Creates additional features that influence similarity between customers.

     Depending on your use-case and the features you have about your customers,
     you may want to do additional feature engineering. For instance, customers
     may purchase products that can be naturally organised into a hierarchy.
     You could code a purchase from "furniture/living/sofa" or from
     "furniture/kitchen/chair" into the following format:

     customer | level_0   | level_1 | level_2
     -----------------------------------------
       0      | furniture | living  | sofa
       1      | furniture | kitchen | chair

    In a later stage, one-hot encoding of these product-levels will result in a
    representation that reflects the similarity between products. If different
    products have different levels of depths, it might be appropriate to decide
    a threshold and drop low levels that occure only rarely.

  Args:
    data: Dataframe to be processed, containing consented and unconsented
      customers.

  Returns:
    Dataframe including new features.
  """
  return data


def preprocess_data(data: pd.DataFrame, drop_columns: List[Any],
                    non_dummy_columns: List[Any],
                    conversion_column: str) -> pd.DataFrame:
  """Preprocesses the passed dataframe.

    Cleans data and applies dummie-coding to relevant columns.

  Args:
    data: Dataframe to be processed (either for consent or no-consent users).
    drop_columns: List of columns to drop from dataframe.
    non_dummy_columns: List of columns to not include in dummy-coding, but keep.
    conversion_column: Name of column indicating conversion value.

  Returns:
    Processed and dummie-coded dataframe
  """
  data = _clean_data(data, conversion_column=conversion_column)
  data = _additional_feature_engineering(data)
  data_dummies = pd.get_dummies(
      data.drop(drop_columns + non_dummy_columns, axis=1, errors="ignore"),
      sparse=True)
  data_dummies.astype(pd.SparseDtype("bool", 0))
  data_dummies = data_dummies.join(data[non_dummy_columns])
  logging.info("Shape of dummy-coded data is:%d", np.shape(data_dummies))
  return data_dummies


def concatenate_and_process_data(
    data_consent: pd.DataFrame,
    data_noconsent: pd.DataFrame,
    conversion_column: str = CONVERSION_COLUMN,
    drop_columns: Tuple[Any, ...] = DROP_COLUMNS,
    non_dummy_columns: Tuple[Any, ...] = NON_DUMMY_COLUMNS
) -> Tuple[pd.DataFrame, pd.DataFrame]:
  """Concatenates consent and no-consent data and preprocesses them.

  Args:
    data_consent: Dataframe of consent customers.
    data_noconsent: Dataframe of no-consent customers.
    conversion_column: Name of the conversion column in the data.
    drop_columns: Names of columns that should be dropped from the data.
    non_dummy_columns: Names of (categorical) columns that should be kept, but
      not dummy-coded.

  Raises:
    ValueError: if concatenating consent and no-consent data doesn't
      match the expected length.

  Returns:
    Processed dataframes for consent and no-consent customers.
  """
  data_noconsent["consent"] = 0
  data_consent["consent"] = 1
  data_concat = pd.concat([data_noconsent, data_consent])
  data_concat.reset_index(inplace=True, drop=True)
  if len(data_concat) != (len(data_noconsent) + len(data_consent)):
    raise ValueError(
        "Length of concatenated data does not match sum of individual dataframes."
    )
  data_preprocessed = preprocess_data(
      data=data_concat,
      drop_columns=list(drop_columns),
      non_dummy_columns=list(non_dummy_columns),
      conversion_column=conversion_column)
  data_noconsent_processed = data_preprocessed[data_preprocessed["consent"] ==
                                               0]
  data_consent_processed = data_preprocessed[data_preprocessed["consent"] == 1]
  return data_consent_processed, data_noconsent_processed
