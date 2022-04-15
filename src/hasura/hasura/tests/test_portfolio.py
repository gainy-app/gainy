import json
import numbers
import os
from common import make_graphql_request, permute_params, PROFILE_ID, MIN_PORTFOLIO_HOLDING_GROUPS_COUNT

# def test_portfolio():
#     query_file = os.path.join(os.path.dirname(__file__),
#                               'queries/GetPortfolioHoldings.graphql')
#     with open(query_file, 'r') as f:
#         query = f.read()
#
#     data = make_graphql_request(query, {"profileId": PROFILE_ID})['data']
#     assert data['portfolio_gains'] is not None
#     assert data['profile_holding_groups'] is not None
#     assert len(
#         data['profile_holding_groups']) >= MIN_PORTFOLIO_HOLDING_GROUPS_COUNT
#
#     for holding_group in data['profile_holding_groups']:
#         assert holding_group['details'] is not None
#         assert holding_group['gains'] is not None
#         assert holding_group['holdings'] is not None
#         assert len(holding_group['holdings']) > 0
#         for holding in holding_group['holdings']:
#             assert holding['holding_details'] is not None
#             assert holding['gains'] is not None
#
#
# def test_portfolio_chart():
#     query_file = os.path.join(os.path.dirname(__file__),
#                               'queries/GetPortfolioChart.graphql')
#     with open(query_file, 'r') as f:
#         query = f.read()
#
#     full_options_dict = {
#         "periods": ["1d"],
#         "accessTokenIds": [None, 1],
#         "accountIds": [None, 7],
#         "institutionIds": [None, 1],
#         "interestIds": [None, 5],
#         "categoryIds": [None, 6],
#         "securityTypes": [None, "equity"],
#         "lttOnly": [None, True],
#     }
#     test_sets = permute_params(full_options_dict)
#
#     for params in test_sets:
#         if "lttOnly" in params:
#             params["lttOnly"] = params["lttOnly"][0]
#
#         data = make_graphql_request(query, {
#             **{
#                 "profileId": PROFILE_ID
#             },
#             **params
#         })['data']
#         if "lttOnly" not in params:
#             assert len(data['get_portfolio_chart']) > 0, json.dumps(params)
#
#     periods = ["1d", "1w", "1m", "3m", "1y", "5y", "all"]
#     for period in periods:
#         data = make_graphql_request(query, {
#             "profileId": PROFILE_ID,
#             "periods": [period]
#         })['data']
#         assert len(data['get_portfolio_chart']) > 0, {"periods": [period]}


def verify_portfolio_chart(portfolio_chart, symbol_charts, quantities, quantities_override=[], assert_message_prefix=""):
    chart_row_index = 0
    for k,chart in symbol_charts.items():
        symbol_chart_datetimes = [i['datetime'] for i in chart]
        break

    portfolio_chart_index = 0
    for chart_row_index,datetime in enumerate(symbol_chart_datetimes):
        expected_value = None
        cur_quantities = quantities

        for start_date, end_date, _quantities in quantities_override:
            if start_date is not None and start_date > datetime:
                continue
            if end_date is not None and end_date < datetime:
                continue

            if isinstance(_quantities, numbers.Number):
                expected_value = _quantities
            else:
                cur_quantities = _quantities

        if expected_value is None:
            expected_value = 0
            for symbol, quantity in cur_quantities.items():
                expected_value += symbol_charts[symbol][chart_row_index][
                    'adjusted_close'] * quantity

        portfolio_chart_row = portfolio_chart[portfolio_chart_index]
        if datetime == portfolio_chart_row['datetime']:
            assert abs(portfolio_chart_row['adjusted_close'] - expected_value) < 1e-3, f"{assert_message_prefix}: wrong value on {datetime}: {portfolio_chart_row['adjusted_close'] }, expected {expected_value}"
            portfolio_chart_index += 1
        else:
            assert abs(expected_value) < 1e-3, f"{assert_message_prefix}: no value on {datetime}, expected {expected_value}"

def verify_profile(user_id, periods, chart_query, profile_ids, charts, quantities, quantities_override=[]):
    profile_id = profile_ids[user_id]
    for period in periods:
        verify_portfolio_chart(
            make_graphql_request(
                chart_query, {
                    "profileId": profile_id,
                    "periods": [period]
                },
                user_id=user_id)['data']['get_portfolio_chart'],
            charts[period], quantities, quantities_override, user_id)


def test_portfolio_data():
    transaction_stats_query = "query transaction_stats($profileId: Int!) {app_profile_portfolio_transactions_aggregate(where: {profile_id: {_eq: $profileId}}) {aggregate{min{date}}}}"
    periods = ["1w", "1m"]

    query = 'query chart($period: String!, $symbol: String!) { chart(where: {symbol: {_eq: $symbol}, period: {_eq: $period}}, order_by: {datetime: asc}) { symbol datetime period open high low close adjusted_close volume } }'
    charts = {
        period: {
            "AAPL":
            make_graphql_request(query, {
                "period": period,
                "symbol": "AAPL"
            })['data']['chart'],
        }
        for period in periods
    }
    quantities = {"AAPL": 100}

    chart_query_file = os.path.join(os.path.dirname(__file__),
                              'queries/GetPortfolioChart.graphql')
    with open(chart_query_file, 'r') as f:
        chart_query = f.read()

    profiles = make_graphql_request("{app_profiles{id, user_id}}",
                                    user_id=None)['data']['app_profiles']
    profile_ids = {profile['user_id']: profile['id'] for profile in profiles}

    # -- profile 1 without holdings without transactions at all
    verify_profile('user_id_portfolio_test_1', periods, chart_query, profile_ids, charts, quantities)

    # -- profile 2 without holdings with one buy transaction on the primary account
    user_id = 'user_id_portfolio_test_2'
    transaction_stats = make_graphql_request(transaction_stats_query, {"profileId": profile_ids[user_id]}, user_id=None)['data']
    quantities_override = [
        (None, transaction_stats['app_profile_portfolio_transactions_aggregate']['aggregate']['min']['date'], 0)
    ]
    verify_profile(user_id, periods, chart_query, profile_ids, charts, quantities, quantities_override)

    # -- profile 3 without holdings with one sell transaction on the primary account
    user_id = 'user_id_portfolio_test_3'
    transaction_stats = make_graphql_request(transaction_stats_query, {"profileId": profile_ids[user_id]}, user_id=None)['data']
    quantities_override = [
        (None, transaction_stats['app_profile_portfolio_transactions_aggregate']['aggregate']['min']['date'], 0),
        (None, transaction_stats['app_profile_portfolio_transactions_aggregate']['aggregate']['min']['date'], {"AAPL": 110})
    ]
    verify_profile(user_id, periods, chart_query, profile_ids, charts, quantities, quantities_override)
