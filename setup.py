# Copyright 2022 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Setup script for package CoCoA."""
import setuptools

with open('README.md', 'r', encoding='utf-8') as fh:
  long_description = fh.read()

setuptools.setup(
    name='consent_based_conversion_adjustments',
    version='0.0.1',
    author='gTech Professional Services',
    author_email='',
    description='Adjust conversion values for Smart Bidding OCI',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=setuptools.find_packages(),
    install_requires=[
        'absl-py==1.0.0',
        'apache-beam==2.28.0',
        'avro-python3==1.9.2.1',
        'cachetools==4.2.4',
        'certifi==2021.10.8',
        'charset-normalizer==2.0.12',
        'crcmod==1.7',
        'dill==0.3.1.1',
        'docopt==0.6.2',
        'fastavro==1.4.9',
        'fasteners==0.17.3',
        'future==0.18.2',
        'google-api-core==1.31.5',
        'google-apitools==0.5.31',
        'google-auth==1.35.0',
        'google-cloud-bigquery==2.34.0',
        'google-cloud-bigquery-storage==2.11.0',
        'google-cloud-bigtable==1.7.0',
        'google-cloud-core==1.7.2',
        'google-cloud-datastore==1.15.3',
        'google-cloud-dlp==3.6.0',
        'google-cloud-language==1.3.0',
        'google-cloud-pubsub==1.7.0',
        'google-cloud-recommendations-ai==0.2.0',
        'google-cloud-spanner==1.19.1',
        'google-cloud-storage==2.1.0',
        'google-cloud-videointelligence==1.16.1',
        'google-cloud-vision==1.0.0',
        'google-crc32c==1.3.0',
        'google-resumable-media==2.2.1',
        'googleapis-common-protos==1.54.0',
        'grpc-google-iam-v1==0.12.3',
        'grpcio==1.44.0',
        'grpcio-gcp==0.2.2',
        'hdfs==2.6.0',
        'httplib2==0.17.4',
        'idna==3.3',
        'joblib==1.1.0',
        'libcst==0.4.1',
        'mock==2.0.0',
        'mypy-extensions==0.4.3',
        'numpy==1.19.5',
        'oauth2client==4.1.3',
        'orjson==3.9.15',
        'packaging==21.3',
        'pandas>=1.3',
        'pbr==5.8.1',
        'proto-plus==1.20.3',
        'protobuf==3.19.4',
        'pyarrow==2.0.0',
        'pyasn1==0.4.8',
        'pyasn1-modules==0.2.8',
        'pydot==1.4.2',
        'pymongo==3.12.3',
        'pyparsing==2.4.7',
        'python-dateutil==2.8.2',
        'pytz==2021.3',
        'PyYAML==6.0',
        'requests==2.27.1',
        'rsa==4.8',
        'scikit-learn==1.0.2',
        'scipy>=1.7',
        'six==1.16.0',
        'sklearn==0.0',
        'threadpoolctl==3.1.0',
        'typing-extensions==3.7.4.3',
        'typing-inspect==0.7.1',
        'urllib3==1.26.8',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.7',
)
