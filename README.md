# Consent-based Conversion Adjustments

## Problem statement

Given regulatory requirements, customers have the choice to accept or decline
third-party cookies. For those who opt-out of third-party cookie tracking
(hereafter, non-consenting customers), data on their conversions on an
advertiser's website cannot be shared with Smart Bidding. This potential data
loss can lead to worse bidding performance, or drifts in the bidding behaviour
away from the advertiser's initial goals.

We have developed a solution that allows advertisers to capitalise on their
first-party data in order to statistically up-weight conversion values of
customers that gave consent. By doing this, advertisers have the possibility to
feed back up to 100% of the factual conversion values back into Smart Bidding.

## Solution description

We take the following approach: For all consenting and non-consenting customers
that converted on a given day, the advertiser has access to first-party data
that describes the customers. Examples could be the adgroup-title that a
conversion is attributed to, the device type used, or demographic information.
Based on this information, a feature space can be created that describes each
consenting and non-consenting customer. Importantly, this feature space has to
be the same for all customers.

Given this feature space, we can create a distance-graph for all *consenting*
customers in our dataset, and find the nearest consenting customers for each
*non-consenting* customer. This is done using a NearestNeighbor model. The
non-consenting customer's conversion value can then be split across all
identified nearest consenting customers, in proportion to the similarity between
the non-consenting and the non-consenting customers.

## Model Parameters

*   Distance metric: We need to define the distance metric to use when
    determining the nearest consenting customers. By default, this is set to
    `manhattan distance`.
*   Radius, number of nearest neighbors, or percentile: In coordination with the
    advertiser and depending on the dataset as well as business requirements,
    the user can choose between:
    *   setting a fixed radius within which all nearest neighbors should be
        selected,
    *   setting a fixed number of nearest neighbors that should be selected for
        each non-consenting customer, independent of their distance to them
    *   finding the required radius to ensure that at least `x%` of
        non-consenting customers would have at least one sufficiently close
        neighbor.

## Data requirements

As mentioned above, consenting and non-consenting customers must lie in the same
feature space. This is currently achieved by considering the adgroup a given
customer has clicked on and splitting it according to the advertiser's logic.
This way, customers that came through similar adgroups are considered being more
similar to each other. All customers to be considered must have a valid
conversion value larger zero and must not have missing data.

## How to use the solution

This solution uses an Apache Beam pipeline to find the nearest consenting
customers for each non-consenting customer. The following instructions show how
to run the pipeline on Google Cloud Dataflow, however any other suitable Apache
Beam runner may be used as well.

### Installation

> Note: This solution requires 3.6 <= Python < 3.9 as Beam does not currently
> support Python 3.9.

#### Set up Dataflow Template

*   Navigate to your Google Cloud Project and activate the Cloud Shell
*   Set the current project by running `gcloud config set project
    [YOUR_PROJECT_ID]`
*   Clone this repository and `cd` into the project directory
*   Download pyenv as described
    [here](https://cwiki.apache.org/confluence/display/BEAM/Python+Tips#PythonTips-VirtualEnvironmentswithpyenv).
*   Create and activate a virtual environment as follows:

    ```
      pyenv install 3.8.12
      pyenv virtualenv 3.8.12 env
      pyenv activate env
    ```

*   Install python3 dependencies `pip3 install -r requirements.txt`

*   Create a GCS bucket (if one does not already exist) where the Dataflow
    template as well as all inputs and outputs will be stored

*   Set an environment variable with the name of the bucket `export
    PIPELINE_BUCKET=[YOUR_CLOUD_STORAGE_BUCKET_NAME]`

*   To read data from BigQuery, we need to know the project containing your
    BigQuery tables. Set an environment variable `export
    BQ_PROJECT_ID=[YOUR_BIGQUERY_PROJECT_ID]`

*   Additionally, set the location of your BigQuery tables `export
    BIGQUERY_LOCATION=[YOUR_BIGQUERY_LOCATION]` e.g. 'EU' for Europe

*   Set an environment variable with the name of your BigQuery table with
    consenting user data `export TABLE_CONSENT=[CONSENTING_USER_TABLE_NAME]`

*   Set an environment variable with the name of your BigQuery table with
    non-consenting user data `export
    TABLE_NOCONSENT=[NON_CONSENTING_USER_TABLE_NAME]`

*   Set an environment variable with the name of the data column in your tables
    `export DATE_COLUMN=[DATA_COLUMN_NAME]`

*   To up-weight the conversion values in our dataset, we need to know which
    column represents the conversion values in the input data. Set an
    environment variable with the name of the conversion column `export
    CONVERSION_COLUMN=[YOUR_CONVERSION_COLUMN_NAME]`

*   The final output of the pipeline is a CSV file that may be used for Offline
    Conversion Import (OCI) into Google Ads or Google Marketing Platform (GMP).
    Each row of this OCI CSV must be unique. Set an environment variable with
    the list of columns in the input data that together form a unique ID `export
    ID_COLUMNS=[COMMA_SEPARATED_ID_COLUMNS_LIST]` e.g. export
    ID_COLUMNS=GCLID,TIMESTAMP,ADGROUP_NAME (**no spaces** between the commas!)

*   You may want to exclude some columns in your data from being used for
    matching. Set an environment variable with the list of columns in the input
    data that should be dropped e.g. `export DROP_COLUMNS=feature2,feature5`

*   Provide all categorical columns in your data that should not be
    [dummy-coded](https://pandas.pydata.org/docs/reference/api/pandas.get_dummies.html)
    e.g. `export non_dummy_columns=GCLID,TIMESTAMP`

*   Set an environment variable with the project id `export
    PROJECT_ID=[YOUR_PROJECT_ID]`

*   Set an environment variable with the
    [regional endpoint](https://cloud.google.com/dataflow/docs/concepts/regional-endpoints)
    to deploy your Dataflow job `export
    PIPELINE_REGION=[YOUR_REGIONAL_ENDPOINT]`

*   Generate the template by running `./generate_template.sh`

*   Deactivate the virtual env by typing `pyenv deactivate` and close cloud
    shell

#### Set up Cloud Function

The Apache Beam pipeline that we set up above will be triggered by a Cloud
Function. Following instructions show how to set up the Cloud Function:

*   Open Cloud Functions from the navigation menu in your Google Cloud Project
*   If not done already, enable the Cloud Functions and Cloud Build APIs
*   Select `Create function` and fill in the required fields such as `Function
    name` and `Region`. Choose Cloud Pub/Sub as a trigger and create a new
    topic. We will later write to this topic whenever the BigQuery tables have
    new data, thereby triggering the cloud function
*   Under runtime setting, set timeout to at least 60 seconds to give ample time
    for the Cloud Function to run. Click next
*   Upload the contents of the `cloud_function` directory found in the repo to
    Cloud Functions
*   Select Python 3.8 as Runtime and set Entry point to "run".
*   Update the required values in `main.py` as marked by `TODO(): ...`
*   Deploy the Cloud Function

#### Set up Cloud Logging to Pub/Sub sink

> Note: For this section, we assume that you wish to trigger the Dataflow
> pipeline whenever new data is inserted in the non-consented or consented
> tables. If you have a different requirement, proceed accordingly with setting
> up a trigger for the Cloud Function. See also:
> [Using Cloud Scheduler and Pub/Sub to trigger a Cloud Function](https://cloud.google.com/scheduler/docs/tut-pub-sub).

*   In Cloud Logging on your Google Cloud Project, filter to the relevant
    BigQuery event. For example, to filter by table inserts, use:

    ```
    protoPayload.serviceName="bigquery.googleapis.com"
    protoPayload.methodName="google.cloud.bigquery.v2.JobService.InsertJob"
    protoPayload.resourceName="projects/[YOUR_PROJECT_ID]/datasets/[YOUR_DATASET]/tables/[YOUR_TABLE_NAME]"
    resource.labels.project_id="[YOUR_PROJECT_ID]"
    protoPayload.metadata.tableDataChange.reason="QUERY"
    ```

    Once the relevant event is available, create a sink that routes your logs to
    the Pub/Sub topic defined above. For more information on creating sinks, see
    the
    [documentation](https://cloud.google.com/logging/docs/export/configure_export_v2).

*   With this in place, the Dataflow pipeline should get triggered whenever new
    data is inserted in your Bigquery tables.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for details.

## License

Apache 2.0; see [`LICENSE`](LICENSE) for details.

## Disclaimer

This is not an official Google product.
