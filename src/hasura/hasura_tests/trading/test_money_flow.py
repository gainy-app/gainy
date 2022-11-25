import pytest

from hasura_tests.common import make_graphql_request
from hasura_tests.trading.common import load_query, PROFILES

profile_id = PROFILES[1]['id']
profile_user_id = PROFILES[1]['user_id']


@pytest.mark.drivewealth
def test_deposit():
    data = make_graphql_request(
        load_query('trading/queries', 'TradingDepositFunds'), {
            "profile_id": profile_id,
            "amount": 10000,
            "funding_account_id": 1
        }, profile_user_id)['data']['trading_deposit_funds']

    assert "trading_money_flow_id" in data
    assert data["trading_money_flow_id"] is not None
