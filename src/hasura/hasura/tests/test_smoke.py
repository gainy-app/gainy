import os
import requests
import json
import datetime
import logging
from common import make_graphql_request, get_personalized_collections, PROFILE_ID, MIN_COLLECTIONS_COUNT, MIN_PERSONALIZED_COLLECTIONS_COUNT, MIN_INTEREST_COUNT, MIN_CATEGORIES_COUNT

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def test_collections():
    query = '{collections(where: {enabled: {_eq: "1"} }) { id name enabled} }'
    data = make_graphql_request(query)['data']['collections']

    logger.info('%d %d', len(data), MIN_COLLECTIONS_COUNT)
    assert len(data) >= MIN_COLLECTIONS_COUNT

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
