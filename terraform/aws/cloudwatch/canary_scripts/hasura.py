import json
import os
import datetime
import http.client
import urllib.parse
from aws_synthetics.selenium import synthetics_webdriver as syn_webdriver
from aws_synthetics.common import synthetics_logger as logger

HASURA_URL = os.getenv("HASURA_URL", "${hasura_url}")
HASURA_ADMIN_SECRET = os.getenv("HASURA_ADMIN_SECRET",
                                "${hasura_admin_secret}")
HASURA_GRAPHQL_URL = "%s/v1/graphql" % (HASURA_URL)
IS_PRODUCTION = HASURA_URL.find('production') > -1
PROFILE_ID = 1
USER_ID = 'AO0OQyz0jyL5lNUpvKbpVdAPvlI3'

MIN_COLLECTIONS_COUNT = 40
MIN_PERSONALIZED_COLLECTIONS_COUNT = 0
MIN_INTEREST_COUNT = 25
MIN_CATEGORIES_COUNT = 5


def make_request(method, url, post_data=None, headers={}):
    parsed_url = urllib.parse.urlparse(url)
    user_agent = str(syn_webdriver.get_canary_user_agent_string())
    if "User-Agent" in headers:
        headers["User-Agent"] = " ".join([user_agent, headers["User-Agent"]])
    else:
        headers["User-Agent"] = "{}".format(user_agent)

    logger.info("%s %s '%s'" % (method, url, post_data['query']))

    if parsed_url.scheme == "https":
        conn = http.client.HTTPSConnection(parsed_url.hostname,
                                           parsed_url.port)
    else:
        conn = http.client.HTTPConnection(parsed_url.hostname, parsed_url.port)

    conn.request(method, url, json.dumps(post_data), headers)
    response = conn.getresponse()
    try:
        response_data = json.loads(response.read().decode())
    except:
        response_data = None
    conn.close()

    logger.info("Status Code: %s " % response.status)
    logger.info("Response Headers: %s" %
                json.dumps(response.headers.as_string()))

    if response_data is None or 'data' not in response_data or not response.status or response.status < 200 or response.status > 299:
        if response_data is not None:
            logger.error("Response: %s" % response_data)

        if response_data is not None and 'errors' in response_data:
            messages = [i['message'] for i in response_data['errors']]
            raise Exception("Failed: %s" % json.dumps(messages))
        elif response.reason != 'OK':
            raise Exception("Failed: %s" % response.reason)
        else:
            raise Exception("Failed with status code: %s" % response.status)

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


def get_personalized_collections():
    query = 'query($profileId: Int!) {collections(where: {enabled: {_eq: "1"}, personalized: {_eq: "1"}, profile_id: {_eq: $profileId} } ) { id name enabled personalized} }'
    data = make_graphql_request(query, {"profileId": PROFILE_ID},
                                None)['data']['collections']

    assert len(data) >= MIN_PERSONALIZED_COLLECTIONS_COUNT
    return data


def get_recommended_collections():
    query = '{ get_recommended_collections(profile_id: %d) { id collection { id name image_url enabled description ticker_collections_aggregate { aggregate { count } } } } }' % (
        PROFILE_ID)
    return make_graphql_request(query)['data']['get_recommended_collections']


def check_collections():
    query = '{collections(where: {enabled: {_eq: "1"} }) { id name enabled} }'
    data = make_graphql_request(query)['data']['collections']

    assert len(data) >= MIN_COLLECTIONS_COUNT

    personalized_collection_ids = set(
        [i['id'] for i in get_personalized_collections()])
    collection_ids = set([i['id'] for i in data])
    assert personalized_collection_ids.issubset(collection_ids)


def check_recommended_collections():
    data = get_recommended_collections()

    assert len(data) >= MIN_PERSONALIZED_COLLECTIONS_COUNT

    personalized_collection_ids = set(
        [i['id'] for i in get_personalized_collections()])
    collection_ids = set([i['id'] for i in data])
    assert personalized_collection_ids.issubset(collection_ids)


def test_favorite_collections():
    data = get_recommended_collections()
    collection_id = data[0]['id']

    query = 'mutation InsertProfileFavoriteCollection($profileID: Int!, $collectionID: Int!){ insert_app_profile_favorite_collections(objects: {collection_id: $collectionID, profile_id: $profileID}, on_conflict: { constraint: profile_favorite_collections_pkey, update_columns: []}) { returning { collection_id } } }'
    make_graphql_request(query, {"profileID": PROFILE_ID, "collectionID": 231})

    query = 'mutation DeleteProfileFavoriteCollection($profileID: Int!, $collectionID: Int!){ delete_app_profile_favorite_collections( where: { collection_id: {_eq: $collectionID}, profile_id: {_eq: $profileID} } ) { returning { collection_id } } }'
    make_graphql_request(query, {"profileID": PROFILE_ID, "collectionID": 231})


def test_collection_metrics():
    data = get_recommended_collections()
    personalized_collection_ids = set(
        [i['id'] for i in get_personalized_collections()])
    non_personalized_collection_ids = set([
        i['id'] for i in data if i['id'] not in personalized_collection_ids
    ][:3])
    collection_ids = non_personalized_collection_ids.union(
        personalized_collection_ids)

    for collection_id in collection_ids:
        query = 'mutation InsertProfileFavoriteCollection($profileID: Int!, $collectionID: Int!){ insert_app_profile_favorite_collections(objects: {collection_id: $collectionID, profile_id: $profileID}, on_conflict: { constraint: profile_favorite_collections_pkey, update_columns: []}) { returning { collection_id } } }'
        make_graphql_request(query, {
            "profileID": PROFILE_ID,
            "collectionID": collection_id
        })

    query_file = os.path.join(os.path.dirname(__file__),
                              'queries/GetHomeTabData.graphql')
    with open(query_file, 'r') as f:
        query = f.read()
    data = make_graphql_request(query, {
        "profileId": PROFILE_ID,
        "rankedCount": 100
    })['data']
    assert len(data['profile_collection_tickers_performance_ranked']) >= 1
    assert len(data['app_profile_favorite_collections']) >= len(collection_ids)
    for i in data['app_profile_favorite_collections']:
        assert i['collection']['metrics']['relative_daily_change'] is not None

    for collection_id in collection_ids:
        query = 'mutation DeleteProfileFavoriteCollection($profileID: Int!, $collectionID: Int!){ delete_app_profile_favorite_collections( where: { collection_id: {_eq: $collectionID}, profile_id: {_eq: $profileID} } ) { returning { collection_id } } }'
        make_graphql_request(query, {
            "profileID": PROFILE_ID,
            "collectionID": collection_id
        })


def check_interests():
    query = '{ interests(where: {enabled: {_eq: "1"} } ) { icon_url id name } }'
    data = make_graphql_request(query)['data']['interests']

    assert len(data) >= MIN_INTEREST_COUNT


def check_categories():
    query = '{ categories { icon_url id name } }'
    data = make_graphql_request(query)['data']['categories']

    assert len(data) >= MIN_CATEGORIES_COUNT


def check_chart():
    query = 'query DiscoverCharts($period: String!, $symbol: String!) { chart(where: {symbol: {_eq: $symbol}, period: {_eq: $period}}, order_by: {datetime: asc}) { symbol datetime period open high low close adjusted_close volume } }'
    datasets = [
        ("1d", 0),
        ("1w", 70),
        ("1m", 15),
    ]

    for (period, min_count) in datasets:
        data = make_graphql_request(query, {
            "period": period,
            "symbol": "AAPL",
        })['data']['chart']

        assert len(data) >= min_count


def check_portfolio():
    with open(
            os.path.join(os.path.dirname(__file__),
                         'queries/GetPlaidHoldings.graphql'), 'r') as f:
        query = f.read()

    data = make_graphql_request(query, {"profileId": PROFILE_ID})['data']
    assert data['portfolio_gains'] is not None
    assert data['profile_holding_groups'] is not None


def handler(event, context):
    logger.info("Selenium Python API canary")

    check_collections()
    check_recommended_collections()
    check_interests()
    check_categories()
    check_chart()
    check_portfolio()

    logger.info("Canary successfully executed")
