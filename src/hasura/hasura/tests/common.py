import os
import re
import requests
import json
import datetime
import logging
import itertools

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

HASURA_GRAPHQL_ADMIN_SECRET = os.getenv('HASURA_GRAPHQL_ADMIN_SECRET')
ENV = os.getenv('ENV')
ENV_LOCAL = 'local'

HASURA_URL = os.getenv("HASURA_URL", "http://localhost:8080")
HASURA_ADMIN_SECRET = os.getenv("HASURA_GRAPHQL_ADMIN_SECRET")
HASURA_GRAPHQL_URL = "%s/v1/graphql" % (HASURA_URL)
PROFILE_ID = 1
USER_ID = 'AO0OQyz0jyL5lNUpvKbpVdAPvlI3'

MIN_COLLECTIONS_COUNT = 40
MIN_PERSONALIZED_COLLECTIONS_COUNT = 1
MIN_INTEREST_COUNT = 25
MIN_CATEGORIES_COUNT = 5
MIN_PORTFOLIO_HOLDING_GROUPS_COUNT = 1

if ENV == ENV_LOCAL:
    MIN_COLLECTIONS_COUNT = 0
    MIN_PERSONALIZED_COLLECTIONS_COUNT = 0


def get_personalized_collections():
    query = 'query($profileId: Int!) {collections(where: {enabled: {_eq: "1"}, personalized: {_eq: "1"}, profile_id: {_eq: $profileId} } ) { id name enabled personalized} }'
    data = make_graphql_request(query, {"profileId": PROFILE_ID},
                                None)['data']['collections']

    assert len(data) >= MIN_PERSONALIZED_COLLECTIONS_COUNT
    return data


def make_request(method, url, post_data=None, headers={}):
    logger.info("%s %s '%s'" %
                (method, url, re.sub(r"\s+", " ", post_data['query'])))
    response = requests.request(method, url, json=post_data, headers=headers)

    try:
        response_data = response.json()
    except:
        response_data = None

    logger.info("Status Code: %s " % response.status_code)

    if response_data is None or 'data' not in response_data or not response.status_code or response.status_code < 200 or response.status_code > 299:
        if response_data is not None:
            logger.error("Response: %s" % response_data)

        if response_data is not None and 'errors' in response_data:
            messages = [i['message'] for i in response_data['errors']]
            raise Exception("Failed: %s" % json.dumps(messages))
        elif response.reason != 'OK':
            raise Exception("Failed: %s" % response.reason)
        else:
            raise Exception("Failed with status code: %s" %
                            response.status_code)

    logger.info("HTTP request successfully executed")

    return response_data


def make_graphql_request(query, variables=None, user_id=USER_ID):
    postData = {
        "query": query,
        "variables": variables,
    }

    headers = {
        "x-hasura-admin-secret": HASURA_ADMIN_SECRET,
        "content-type": "application/json",
    }

    if user_id is not None:
        headers["x-hasura-user-id"] = user_id
        headers["x-hasura-role"] = "user"

    return make_request('POST', HASURA_GRAPHQL_URL, postData, headers)


def permute_params(full_options_dict):
    test_sets = []
    for k, possible_values in full_options_dict.items():
        options = []

        index_start = 0
        if len(possible_values) > 0 and possible_values[0] is None:
            options.append({})
            index_start = 1

        for index_end in range(index_start, len(possible_values)):
            options.append({k: possible_values[index_start:index_end + 1]})

        test_sets.append(options)

    test_sets = itertools.product(*test_sets)
    for test_set in test_sets:
        d = {}
        for j in test_set:
            d.update(j)
        yield d
