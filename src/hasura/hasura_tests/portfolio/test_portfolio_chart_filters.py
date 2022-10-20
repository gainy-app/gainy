import json
import os
import pytest
from hasura_tests.common import make_graphql_request, permute_params, PROFILE_ID


def get_test_portfolio_chart_filters_data():
    query = """{
        app_profile_plaid_access_tokens { id }
        app_plaid_institutions(where: {name: {_eq: "Demo"}}) {id}
        portfolio_brokers(where: {name: {_eq: "Demo"}}) {uniq_id}
    }"""
    data = make_graphql_request(query)['data']
    access_token_id = data['app_profile_plaid_access_tokens'][0]['id']
    institution_id = data['app_plaid_institutions'][0]['id']
    broker_id = data['portfolio_brokers'][0]['uniq_id']

    full_options_dict = {
        "periods": ["1d"],
        "accessTokenIds": [None, access_token_id],
        "institutionIds": [None, institution_id],
        "brokerIds": [None, broker_id],
        "interestIds": [None, 12],
        "categoryIds": [None, 6],
        "securityTypes": [None, "equity"],
        "lttOnly": [None, True],
    }

    for params in permute_params(full_options_dict):
        yield params

    periods = ["1d", "1w", "1m", "3m", "1y", "5y", "all"]
    for period in periods:
        yield {"periods": [period]}


@pytest.mark.parametrize("params", get_test_portfolio_chart_filters_data())
def test_portfolio_chart_filters(params):
    query_file = os.path.join(os.path.dirname(__file__),
                              'queries/GetPortfolioChart.graphql')
    with open(query_file, 'r') as f:
        query = f.read()

    if "lttOnly" in params:
        params["lttOnly"] = params["lttOnly"][0]

    data = make_graphql_request(query, {
        "profileId": PROFILE_ID,
        **params
    })['data']
    if "lttOnly" not in params:
        assert len(data['get_portfolio_chart']) > 0, json.dumps(params)
        for period in ['1d', '1w', '1m', '3m', '1y', '5y']:
            assert f'prev_close_{period}' in data[
                'get_portfolio_chart_previous_period_close'], json.dumps(
                    params)
