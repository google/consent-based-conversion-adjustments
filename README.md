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

[go/cocoa-slides](http://go/cocoa-slides) We take the following approach: For
all consenting and non-consenting customers that converted on a given day, the
advertiser has access to first-party data that describes the customers. Examples
could be the adgroup-title that a conversion is attributed to, the device type
used, or demographic information. Based on this information, a feature space can
be created that describes each consenting and non-consenting customer.
Importantly, this feature space has to be the same for all customers.

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
