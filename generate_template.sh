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
    --runner DataflowRunner \
    --project "${DEVSHELL_PROJECT_ID}" \
    --region "${PIPELINE_REGION}" \
    --temp_location "gs://${PIPELINE_BUCKET}/temp/" \
    --staging_location "gs://${PIPELINE_BUCKET}/staging/" \
    --template_location "gs://${PIPELINE_BUCKET}/templates/cocoa-template" \
    --output_csv_path "gs://${PIPELINE_BUCKET}/output/adjusted-conversions" \
    --conversion_column "${CONVERSION_COLUMN}" \
    --id_columns "${ID_COLUMNS}" \
    --autoscaling_algorithm "THROUGHPUT_BASED" \
    --machine_type "n1-highmem-32" \
    --setup_file ./setup.py

echo "Done."