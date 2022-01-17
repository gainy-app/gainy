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
PROFILE_ID = 1
USER_ID = 'AO0OQyz0jyL5lNUpvKbpVdAPvlI3'

MIN_COLLECTIONS_COUNT = 200
MIN_PERSONALIZED_COLLECTIONS_COUNT = 1
MIN_INTEREST_COUNT = 25
MIN_CATEGORIES_COUNT = 5
MIN_PORTFOLIO_HOLDING_GROUPS_COUNT = 1


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
        if response.reason:
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


def check_collections():
    query = '{collections(where: {enabled: {_eq: "1"} }) { id name enabled} }'
    data = make_graphql_request(query)['data']['collections']

    assert len(data) >= MIN_COLLECTIONS_COUNT

    personalized_collection_ids = set(
        [i['id'] for i in get_personalized_collections()])
    collection_ids = set([i['id'] for i in data])
    assert personalized_collection_ids.issubset(collection_ids)


def check_recommended_collections():
    query = '{ get_recommended_collections(profile_id: %d) { id collection { id name image_url enabled description ticker_collections_aggregate { aggregate { count } } } } }' % (
        PROFILE_ID)
    data = make_graphql_request(query)['data']['get_recommended_collections']

    assert len(data) >= MIN_PERSONALIZED_COLLECTIONS_COUNT

    personalized_collection_ids = set(
        [i['id'] for i in get_personalized_collections()])
    collection_ids = set([i['id'] for i in data])
    assert personalized_collection_ids.issubset(collection_ids)


def check_interests():
    query = '{ interests(where: {enabled: {_eq: "1"} } ) { icon_url id name } }'
    data = make_graphql_request(query)['data']['interests']

    assert len(data) >= MIN_INTEREST_COUNT


def check_categories():
    query = '{ categories { icon_url id name } }'
    data = make_graphql_request(query)['data']['categories']

    assert len(data) >= MIN_CATEGORIES_COUNT


def check_chart():
    query = 'query DiscoverCharts($period: String!, $symbol: String!, $dateG: timestamp!, $dateL: timestamp!) { historical_prices_aggregated(where: {symbol: {_eq: $symbol}, period: {_eq: $period}, datetime: {_gte: $dateG, _lte: $dateL}}, order_by: {datetime: asc}) { symbol datetime period open high low close adjusted_close volume } }'
    datasets = [
        ("1d", datetime.datetime.now() - datetime.timedelta(days=10), 5),
        ("1w", datetime.datetime.now() - datetime.timedelta(days=30), 4),
        ("1m", datetime.datetime.now() - datetime.timedelta(days=200), 6),
    ]
    now = datetime.datetime.now()

    for (period, date_from, min_count) in datasets:
        data = make_graphql_request(
            query, {
                "period": period,
                "symbol": "AAPL",
                "dateG": date_from.isoformat(),
                "dateL": now.isoformat(),
            })['data']['historical_prices_aggregated']

        assert len(data) >= min_count


def check_portfolio():
    query = '{ app_profile_plaid_access_tokens(distinct_on: [profile_id], where: {profile: {email: {_in: ["test3@example.com", "info@gainy.app", "boris@gainy.app"]} } } ) { profile{ id user_id } } }'
    profiles = make_graphql_request(
        query, None, None)['data']['app_profile_plaid_access_tokens']

    with open(
            os.path.join(os.path.dirname(__file__),
                         'queries/GetPlaidHoldings.graphql'), 'r') as f:
        query = f.read()

    for profile in profiles:
        data = make_graphql_request(query,
                                    {"profileId": profile['profile']['id']},
                                    profile['profile']['user_id'])['data']

        assert data['portfolio_gains'] is not None
        assert data['profile_holding_groups'] is not None
        assert len(data['profile_holding_groups']
                   ) >= MIN_PORTFOLIO_HOLDING_GROUPS_COUNT

        for holding_group in data['profile_holding_groups']:
            assert holding_group['details'] is not None
            assert holding_group['gains'] is not None
            assert holding_group['holdings'] is not None
            assert len(holding_group['holdings']) > 0
            for holding in holding_group['holdings']:
                assert holding['holding_details'] is not None
                assert holding['gains'] is not None


def handler(event, context):
    logger.info("Selenium Python API canary")

    check_collections()
    check_recommended_collections()
    check_interests()
    check_categories()
    check_chart()
    check_portfolio()

    logger.info("Canary successfully executed")
