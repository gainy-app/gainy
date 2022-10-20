import json
import os
import pytest
from hasura_tests.common import make_graphql_request, permute_params, PROFILE_ID, PRICE_EPS


def get_test_portfolio_piechart_filters_data():
    query = """{
        app_profile_plaid_access_tokens { id }
        portfolio_brokers(where: {name: {_eq: "Demo"}}) {uniq_id}
    }"""
    data = make_graphql_request(query)['data']
    access_token_id = data['app_profile_plaid_access_tokens'][0]['id']
    broker_id = data['portfolio_brokers'][0]['uniq_id']

    full_options_dict = {
        "accessTokenIds": [None, access_token_id],
        "brokerIds": [None, broker_id],
    }

    for params in permute_params(full_options_dict):
        yield params


@pytest.mark.parametrize("params", get_test_portfolio_piechart_filters_data())
def test_portfolio_piechart_filters(params):
    query_file = os.path.join(os.path.dirname(__file__),
                              'queries/GetPortfolioPieChart.graphql')
    with open(query_file, 'r') as f:
        query = f.read()

    portfolio_gains_data = make_graphql_request(
        "query GetPortfolioHoldings($profileId: Int!) {portfolio_gains (where:{profile_id: {_eq: $profileId}}) { absolute_gain_1d actual_value } }",
        {
            "profileId": PROFILE_ID,
        })['data']['portfolio_gains'][0]

    data = make_graphql_request(query, {
        "profileId": PROFILE_ID,
        **params
    })['data']

    assert len(data['get_portfolio_piechart']) > 0, json.dumps(params)

    piechart_sums = {}
    for row in data['get_portfolio_piechart']:
        entity_type = row['entity_type']
        if entity_type not in piechart_sums:
            piechart_sums[entity_type] = {}

        expected_relative_daily_change = row['absolute_value'] / (
            row['absolute_value'] - row['absolute_daily_change']) - 1
        assert abs(expected_relative_daily_change -
                   row['relative_daily_change']) < PRICE_EPS, row
        for field in ['weight', 'absolute_daily_change', 'absolute_value']:
            if field not in piechart_sums[entity_type]:
                piechart_sums[entity_type][field] = 0
            piechart_sums[entity_type][field] += row[field]

    print(data, piechart_sums)
    entity_types = [
        'ticker', 'category', 'interest', 'security_type', 'collection'
    ]
    for entity_type in entity_types:
        assert abs(
            1 - piechart_sums[entity_type]['weight']) < PRICE_EPS, entity_type

        if entity_type not in ['category', 'interest', 'collection']:
            assert abs(portfolio_gains_data['absolute_gain_1d'] -
                       piechart_sums[entity_type]['absolute_daily_change']
                       ) < PRICE_EPS, entity_type
            assert abs(portfolio_gains_data['actual_value'] -
                       piechart_sums[entity_type]['absolute_value']
                       ) < PRICE_EPS, entity_type
