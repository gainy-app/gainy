import pytest

from hasura_tests.common import make_graphql_request
from hasura_tests.trading.common import load_query, PROFILES

profile_id = PROFILES[1]['id']
profile_user_id = PROFILES[1]['user_id']


@pytest.mark.drivewealth
def test_profile_status():
    data = make_graphql_request(
        load_query('trading/queries', 'TradingGetProfileStatus'), {
            "profile_id": profile_id,
        }, profile_user_id)['data']

    assert "trading_profile_status" in data
    assert "app_trading_money_flow" in data
    assert "withdrawable_cash" in data['trading_profile_status'][0]
    assert "buying_power" in data['trading_profile_status'][0]
    assert "pending_cash" in data['trading_profile_status'][0]
    assert "pending_orders_count" in data['trading_profile_status'][0]
