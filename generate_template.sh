#!/usr/bin/env bash
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

echo "Requesting template to be generated..."

python -m pipeline \
    --input_path "gs://${PIPELINE_BUCKET}/input/dates.txt" \
    --output_csv_bucket "${PIPELINE_BUCKET}" \
    --output_csv_path "output" \
    --bq_project "${BQ_PROJECT_ID}" \
    --location "${BIGQUERY_LOCATION}" \
    --table_consent "${TABLE_CONSENT}" \
    --table_noconsent "${TABLE_NOCONSENT}" \
    --date_column "${DATE_COLUMN}" \
    --conversion_column "${CONVERSION_COLUMN}" \
    --id_columns "${ID_COLUMNS}" \
    --drop_columns "${DROP_COLUMNS}" \
    --non_dummy_columns "${NON_DUMMY_COLUMNS}" \
    --runner DataflowRunner \
    --project "${PROJECT_ID}" \
    --staging_location "gs://${PIPELINE_BUCKET}/staging/" \
    --temp_location "gs://${PIPELINE_BUCKET}/temp/" \
    --template_location "gs://${PIPELINE_BUCKET}/templates/cocoa-template" \
    --region "${PIPELINE_REGION}" \
    --machine_type "n1-highmem-32" \
    --setup_file ./setup.py

echo "Done."
