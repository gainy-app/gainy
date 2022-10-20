import os
from hasura_tests.common import make_graphql_request, PROFILE_ID, MIN_PORTFOLIO_HOLDING_GROUPS_COUNT
from hasura_tests.portfolio.common import PROFILES


def test_portfolio():
    query_file = os.path.join(os.path.dirname(__file__),
                              'queries/GetPortfolioHoldings.graphql')
    with open(query_file, 'r') as f:
        query = f.read()

    data = make_graphql_request(query, {"profileId": PROFILE_ID})['data']
    assert data['portfolio_gains'] is not None
    assert data['profile_holding_groups'] is not None
    assert len(
        data['profile_holding_groups']) >= MIN_PORTFOLIO_HOLDING_GROUPS_COUNT


def test_demo_portfolio():
    query_file = os.path.join(os.path.dirname(__file__),
                              'queries/GetPortfolioHoldings.graphql')
    with open(query_file, 'r') as f:
        query = f.read()

    profile_ids = {profile['id']: profile['user_id'] for profile in PROFILES}
    data = make_graphql_request(query, {"profileId": PROFILE_ID},
                                profile_ids[2])['data']
    assert data['portfolio_gains'] is not None
    assert data['profile_holding_groups'] is not None
    assert len(
        data['profile_holding_groups']) >= MIN_PORTFOLIO_HOLDING_GROUPS_COUNT
