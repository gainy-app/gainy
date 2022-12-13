import pytest

from hasura_tests.common import make_graphql_request
from hasura_tests.trading.common import load_query, PROFILES

profile_id = PROFILES[1]['id']
profile_user_id = PROFILES[1]['user_id']


def get_test_create_stock_order_target_amount_delta():
    return [100, -90]


@pytest.mark.drivewealth
@pytest.mark.parametrize("target_amount_delta",
                         get_test_create_stock_order_target_amount_delta())
def test_create_stock_order(target_amount_delta):
    data = make_graphql_request(
        load_query('trading/queries', 'TradingCreateStockOrder'), {
            "profile_id": profile_id,
            "symbol": "AAPL",
            "target_amount_delta": target_amount_delta
        }, profile_user_id)['data']['trading_create_stock_order']

    assert "trading_order_id" in data
    assert data["trading_order_id"] is not None

    # TODO check portfolio data changes
    # TODO check history entities change
