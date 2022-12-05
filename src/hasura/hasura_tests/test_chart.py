import logging
from hasura_tests.common import make_graphql_request

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def test_chart():
    query = 'query DiscoverCharts($period: String!, $symbol: String!) { chart(where: {symbol: {_eq: $symbol}, period: {_eq: $period}}, order_by: {datetime: asc}) { symbol datetime period open high low close adjusted_close volume } }'
    datasets = [
        ("1d", 0),
        ("1w", 60),
        ("1m", 15),
    ]

    for (period, min_count) in datasets:
        data = make_graphql_request(query, {
            "period": period,
            "symbol": "AAPL",
        })['data']['chart']

        assert len(data) >= min_count
