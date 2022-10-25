import pytest

from hasura_tests.common import make_graphql_request
from hasura_tests.trading.common import load_query, PROFILES

profile_id = PROFILES[1]['id']
profile_user_id = PROFILES[1]['user_id']


@pytest.mark.drivewealth
def test_get_profile_data():
    data = make_graphql_request(
        load_query('trading/queries', 'TradingGetProfileData'), {
            "profile_id": profile_id,
        }, profile_user_id)['data']['trading_get_profile_data']

    assert "history" in data
    assert "pending" in data["history"]
    assert "withdrawable_cash" in data
    assert "buying_power" in data
