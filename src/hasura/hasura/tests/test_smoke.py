import os
import requests
import json
import datetime
import logging
from common import make_graphql_request, PROFILE_ID, ENV, ENV_LOCAL

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

MIN_COLLECTIONS_COUNT = 200
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


def test_collections():
    query = '{collections(where: {enabled: {_eq: "1"} }) { id name enabled} }'
    data = make_graphql_request(query)['data']['collections']

    logger.info('%d %d', len(data), MIN_COLLECTIONS_COUNT)
    assert len(data) >= MIN_COLLECTIONS_COUNT

    personalized_collection_ids = set(
        [i['id'] for i in get_personalized_collections()])
    collection_ids = set([i['id'] for i in data])
    assert personalized_collection_ids.issubset(collection_ids)


def test_recommended_collections():
    query = '{ get_recommended_collections(profile_id: %d) { id collection { id name image_url enabled description ticker_collections_aggregate { aggregate { count } } } } }' % (
        PROFILE_ID)
    data = make_graphql_request(query)['data']['get_recommended_collections']

    assert len(data) >= MIN_PERSONALIZED_COLLECTIONS_COUNT

    personalized_collection_ids = set(
        [i['id'] for i in get_personalized_collections()])
    collection_ids = set([i['id'] for i in data])
    assert personalized_collection_ids.issubset(collection_ids)


def test_interests():
    query = '{ interests(where: {enabled: {_eq: "1"} } ) { icon_url id name } }'
    data = make_graphql_request(query)['data']['interests']

    assert len(data) >= MIN_INTEREST_COUNT


def test_categories():
    query = '{ categories { icon_url id name } }'
    data = make_graphql_request(query)['data']['categories']

    assert len(data) >= MIN_CATEGORIES_COUNT


def test_chart():
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
