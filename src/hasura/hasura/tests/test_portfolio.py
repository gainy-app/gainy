import json
import numbers
import os
import pytest
from common import make_graphql_request, permute_params, PROFILE_ID, MIN_PORTFOLIO_HOLDING_GROUPS_COUNT

PROFILES = make_graphql_request("{app_profiles{id, user_id}}",
                                user_id=None)['data']['app_profiles']
PROFILE_IDS = {profile['user_id']: profile['id'] for profile in PROFILES}


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
        "accessTokenIds": [None, 1],
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


def verify_portfolio_chart(portfolio_chart,
                           symbol_charts,
                           quantities,
                           quantities_override=[],
                           assert_message_prefix=""):
    chart_row_index = 0
    for k, chart in symbol_charts.items():
        symbol_chart_datetimes = [i['datetime'] for i in chart]
        break

    portfolio_chart_index = 0
    for chart_row_index, datetime in enumerate(symbol_chart_datetimes):
        cur_quantities = quantity_override = None

        for start_date, end_date, _quantities in quantities_override:
            if start_date is not None and start_date > datetime:
                continue
            if end_date is not None and end_date < datetime:
                continue

            if isinstance(_quantities, numbers.Number):
                quantity_override = _quantities
            else:
                cur_quantities = _quantities
            break

        expected_value = 0
        for symbol, quantity in quantities.items():
            if quantity_override is not None:
                quantity = quantity_override
            if cur_quantities is not None:
                quantity = cur_quantities[symbol]

            expected_value += symbol_charts[symbol][chart_row_index][
                'adjusted_close'] * quantity

        if portfolio_chart_index < len(portfolio_chart):
            portfolio_chart_row = portfolio_chart[portfolio_chart_index]
        else:
            portfolio_chart_row = None

        if portfolio_chart_row is not None and datetime == portfolio_chart_row[
                'datetime']:
            assert abs(
                portfolio_chart_row['adjusted_close'] - expected_value
            ) < 1e-3, f"{assert_message_prefix}: wrong value on {datetime}: {portfolio_chart_row['adjusted_close'] }, expected {expected_value}"
            portfolio_chart_index += 1
        else:
            assert abs(
                expected_value
            ) < 1e-3, f"{assert_message_prefix}: no value on {datetime}, expected {expected_value}"


def verify_profile(user_id,
                   periods,
                   chart_query,
                   PROFILE_IDS,
                   charts,
                   quantities,
                   quantities_override=[]):
    profile_id = PROFILE_IDS[user_id]
    for period in periods:
        verify_portfolio_chart(
            make_graphql_request(
                chart_query, {
                    "profileId": profile_id,
                    "periods": [period]
                },
                user_id=user_id)['data']['get_portfolio_chart'],
            charts[period], quantities, quantities_override, user_id)


def get_test_portfolio_data():
    transaction_stats_query = "query transaction_stats($profileId: Int!) {app_profile_portfolio_transactions_aggregate(where: {profile_id: {_eq: $profileId}}) {aggregate{min{date} max{date}}}}"
    quantities = {"AAPL": 100}

    # -- profile 1 with holdings without transactions at all
    yield ('user_id_portfolio_test_1', quantities, [])

    # -- profile 2 with holdings with one buy transaction on the primary account
    user_id = 'user_id_portfolio_test_2'
    transaction_stats = make_graphql_request(
        transaction_stats_query, {"profileId": PROFILE_IDS[user_id]},
        user_id=None)['data']
    quantities_override = [
        (None,
         transaction_stats['app_profile_portfolio_transactions_aggregate']
         ['aggregate']['min']['date'], 0),
    ]
    yield (user_id, quantities, quantities_override)

    # -- profile 3 with holdings with one sell transaction on the primary account
    user_id = 'user_id_portfolio_test_3'
    transaction_stats = make_graphql_request(
        transaction_stats_query, {"profileId": PROFILE_IDS[user_id]},
        user_id=None)['data']
    quantities_override = [
        (None,
         transaction_stats['app_profile_portfolio_transactions_aggregate']
         ['aggregate']['min']['date'], 0),
    ]
    yield (user_id, quantities, quantities_override)

    # -- profile 4 with holdings with one buy transaction on the secondary account
    user_id = 'user_id_portfolio_test_4'
    transaction_stats = make_graphql_request(
        transaction_stats_query, {"profileId": PROFILE_IDS[user_id]},
        user_id=None)['data']
    quantities_override = [
        (None,
         transaction_stats['app_profile_portfolio_transactions_aggregate']
         ['aggregate']['min']['date'], 0),
    ]
    yield (user_id, quantities, quantities_override)

    # -- profile 5 with holdings with one sell transaction on the secondary account
    user_id = 'user_id_portfolio_test_5'
    transaction_stats = make_graphql_request(
        transaction_stats_query, {"profileId": PROFILE_IDS[user_id]},
        user_id=None)['data']
    quantities_override = [
        (None,
         transaction_stats['app_profile_portfolio_transactions_aggregate']
         ['aggregate']['min']['date'], 0),
    ]
    yield (user_id, quantities, quantities_override)

    # -- profile 6 with holdings with buy-sell transactions on the primary account
    user_id = 'user_id_portfolio_test_6'
    transaction_stats = make_graphql_request(
        transaction_stats_query, {"profileId": PROFILE_IDS[user_id]},
        user_id=None)['data']
    quantities_override = [
        (None,
         transaction_stats['app_profile_portfolio_transactions_aggregate']
         ['aggregate']['min']['date'], 0),
        (transaction_stats['app_profile_portfolio_transactions_aggregate']
         ['aggregate']['min']['date'],
         transaction_stats['app_profile_portfolio_transactions_aggregate']
         ['aggregate']['max']['date'], 110),
    ]
    yield (user_id, quantities, quantities_override)

    # -- profile 7 with holdings with buy-sell transactions on the primary-secondary account
    user_id = 'user_id_portfolio_test_7'
    transaction_stats = make_graphql_request(
        transaction_stats_query, {"profileId": PROFILE_IDS[user_id]},
        user_id=None)['data']
    quantities_override = [
        (None,
         transaction_stats['app_profile_portfolio_transactions_aggregate']
         ['aggregate']['min']['date'], 0),
        (transaction_stats['app_profile_portfolio_transactions_aggregate']
         ['aggregate']['min']['date'],
         transaction_stats['app_profile_portfolio_transactions_aggregate']
         ['aggregate']['max']['date'], 110),
    ]
    yield (user_id, quantities, quantities_override)

    # -- profile 8 with holdings with buy-sell transactions on the secondary-primary account
    user_id = 'user_id_portfolio_test_8'
    transaction_stats = make_graphql_request(
        transaction_stats_query, {"profileId": PROFILE_IDS[user_id]},
        user_id=None)['data']
    quantities_override = [
        (None,
         transaction_stats['app_profile_portfolio_transactions_aggregate']
         ['aggregate']['min']['date'], 0),
        (transaction_stats['app_profile_portfolio_transactions_aggregate']
         ['aggregate']['min']['date'],
         transaction_stats['app_profile_portfolio_transactions_aggregate']
         ['aggregate']['max']['date'], 110),
    ]
    yield (user_id, quantities, quantities_override)

    # -- profile 9 with holdings with buy-sell transactions on the secondary account
    user_id = 'user_id_portfolio_test_9'
    transaction_stats = make_graphql_request(
        transaction_stats_query, {"profileId": PROFILE_IDS[user_id]},
        user_id=None)['data']
    quantities_override = [
        (None,
         transaction_stats['app_profile_portfolio_transactions_aggregate']
         ['aggregate']['min']['date'], 0),
        (transaction_stats['app_profile_portfolio_transactions_aggregate']
         ['aggregate']['min']['date'],
         transaction_stats['app_profile_portfolio_transactions_aggregate']
         ['aggregate']['max']['date'], 110),
    ]
    yield (user_id, quantities, quantities_override)

    # -- profile 10 with holdings with sell-buy transactions on the primary account
    user_id = 'user_id_portfolio_test_10'
    transaction_stats = make_graphql_request(
        transaction_stats_query, {"profileId": PROFILE_IDS[user_id]},
        user_id=None)['data']
    quantities_override = [
        (None,
         transaction_stats['app_profile_portfolio_transactions_aggregate']
         ['aggregate']['max']['date'], 0),
    ]
    yield (user_id, quantities, quantities_override)

    # -- profile 11 with holdings with sell-buy transactions on the primary-secondary account
    user_id = 'user_id_portfolio_test_11'
    transaction_stats = make_graphql_request(
        transaction_stats_query, {"profileId": PROFILE_IDS[user_id]},
        user_id=None)['data']
    quantities_override = [
        (None,
         transaction_stats['app_profile_portfolio_transactions_aggregate']
         ['aggregate']['min']['date'], 0),
    ]
    yield (user_id, quantities, quantities_override)

    # -- profile 12 with holdings with sell-buy transactions on the secondary-primary account
    user_id = 'user_id_portfolio_test_12'
    transaction_stats = make_graphql_request(
        transaction_stats_query, {"profileId": PROFILE_IDS[user_id]},
        user_id=None)['data']
    quantities_override = [
        (None,
         transaction_stats['app_profile_portfolio_transactions_aggregate']
         ['aggregate']['max']['date'], 0),
    ]
    yield (user_id, quantities, quantities_override)

    # -- profile 13 with holdings with sell-buy transactions on the secondary account
    user_id = 'user_id_portfolio_test_13'
    transaction_stats = make_graphql_request(
        transaction_stats_query, {"profileId": PROFILE_IDS[user_id]},
        user_id=None)['data']
    quantities_override = [
        (None,
         transaction_stats['app_profile_portfolio_transactions_aggregate']
         ['aggregate']['min']['date'], 0),
    ]
    yield (user_id, quantities, quantities_override)

    quantities = {"AAPL": 0}

    # -- profile 14 without holdings without transactions at all
    # -- profile 15 without holdings with one buy transaction on the primary account
    # -- profile 16 without holdings with one sell transaction on the primary account
    # -- profile 17 without holdings with one buy transaction on the secondary account
    # -- profile 18 without holdings with one sell transaction on the secondary account
    # -- profile 19 without holdings with buy-sell transactions on the primary account
    # -- profile 20 without holdings with buy-sell transactions on the primary-secondary account
    # -- profile 21 without holdings with buy-sell transactions on the secondary-primary account
    # -- profile 22 without holdings with buy-sell transactions on the secondary account
    # -- profile 23 without holdings with sell-buy transactions on the primary account
    # -- profile 24 without holdings with sell-buy transactions on the primary-secondary account
    # -- profile 25 without holdings with sell-buy transactions on the secondary-primary account
    # -- profile 26 without holdings with sell-buy transactions on the secondary account
    for i in range(14, 27):
        if i < 19 or i > 22:
            yield ('user_id_portfolio_test_' + str(i), quantities, [])
        else:
            transaction_stats = make_graphql_request(
                transaction_stats_query, {"profileId": PROFILE_IDS[user_id]},
                user_id=None)['data']
            quantities_override = [
                (transaction_stats[
                    'app_profile_portfolio_transactions_aggregate']
                 ['aggregate']['min']['date'], transaction_stats[
                     'app_profile_portfolio_transactions_aggregate']
                 ['aggregate']['max']['date'], 10),
            ]
            yield ('user_id_portfolio_test_' + str(i), quantities,
                   quantities_override)


@pytest.mark.parametrize("user_id,quantities,quantities_override",
                         get_test_portfolio_data())
def test_portfolio_data(user_id, quantities, quantities_override):
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

    chart_query_file = os.path.join(os.path.dirname(__file__),
                                    'queries/GetPortfolioChart.graphql')
    with open(chart_query_file, 'r') as f:
        chart_query = f.read()

    verify_profile(user_id, periods, chart_query, PROFILE_IDS, charts,
                   quantities, quantities_override)
