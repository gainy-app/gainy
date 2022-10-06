import pytest

from hasura_tests.common import make_graphql_request
from hasura_tests.trading.common import load_query, PROFILES

profile_id = PROFILES[1]['id']
profile_user_id = PROFILES[1]['user_id']


def get_test_reconfigure_collection_holdings_target_amount_delta():
    return [100, -100]


@pytest.mark.drivewealth
@pytest.mark.parametrize(
    "target_amount_delta",
    get_test_reconfigure_collection_holdings_target_amount_delta())
def test_reconfigure_collection_holdings(target_amount_delta):
    data = make_graphql_request(
        load_query('trading/queries', 'TradingReconfigureCollectionHoldings'),
        {
            "profile_id": profile_id,
            "collection_id": 89,
            "weights": [{
                "symbol": "AAPL",
                "weight": 1
            }],
            "target_amount_delta": target_amount_delta
        }, profile_user_id)['data']['trading_reconfigure_collection_holdings']

    assert "trading_collection_version_id" in data
    assert data["trading_collection_version_id"] is not None

    # TODO check portfolio data changes
    # TODO check history entities change
