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

"""Create mock data and define global variables used across multiple tests."""

import numpy as np
import pandas as pd

CONVERSION_COLUMN = 'conversion_column'
ID_COLUMNS = ['id_column']

product_levels = ['1_1', '2_2', '1_1']
DATA_CONSENT = pd.concat([
    pd.DataFrame(
        np.array([[1, 2, 3, 0], [0, 5, 6, 0], [1, 8, 9, 0]]),
        columns=['a', 'b', CONVERSION_COLUMN] + ID_COLUMNS)
] * 10)
DATA_CONSENT.reset_index(inplace=True, drop=True)
DATA_CONSENT['product_level'] = product_levels * 10
DATA_NOCONSENT = pd.concat([
    pd.DataFrame(
        np.array([[4, 5, 6, 0], [7, 8, 9, 0], [10, 11, 12, 0]]),
        columns=['a', 'b', CONVERSION_COLUMN] + ID_COLUMNS)
] * 5)
DATA_NOCONSENT.reset_index(inplace=True, drop=True)
DATA_NOCONSENT['product_level'] = product_levels * 5

METRIC = 'manhattan'
