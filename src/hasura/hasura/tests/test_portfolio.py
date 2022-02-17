import json
import os
from common import make_graphql_request, permute_params, PROFILE_ID, MIN_PORTFOLIO_HOLDING_GROUPS_COUNT


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

    for holding_group in data['profile_holding_groups']:
        assert holding_group['details'] is not None
        assert holding_group['gains'] is not None
        assert holding_group['holdings'] is not None
        assert len(holding_group['holdings']) > 0
        for holding in holding_group['holdings']:
            assert holding['holding_details'] is not None
            assert holding['gains'] is not None


def test_portfolio_chart():
    query_file = os.path.join(os.path.dirname(__file__),
                              'queries/GetPortfolioChart.graphql')
    with open(query_file, 'r') as f:
        query = f.read()

    full_options_dict = {
        "periods": ["1d"],
        "accountIds": [None, 7],
        "institutionIds": [None, 1],
        "interestIds": [None, 5],
        "categoryIds": [None, 6],
        "securityTypes": [None, "equity"],
        "lttOnly": [None, True],
    }
    test_sets = permute_params(full_options_dict)

    for params in test_sets:
        if "lttOnly" in params:
            params["lttOnly"] = params["lttOnly"][0]

        data = make_graphql_request(query, {
            **{
                "profileId": PROFILE_ID
            },
            **params
        })['data']
        if "lttOnly" not in params:
            assert len(data['get_portfolio_chart']) > 0, json.dumps(params)

    periods = ["1d", "1w", "1m", "3m", "1y", "5y", "all"]
    for period in periods:
        data = make_graphql_request(query, {
            "profileId": PROFILE_ID,
            "periods": [period]
        })['data']
        assert len(data['get_portfolio_chart']) > 0, {"periods": [period]}
