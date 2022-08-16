import datetime
import dateutil.relativedelta
import json
import numbers
import os
import pytest
from common import make_graphql_request, permute_params, PROFILE_ID, MIN_PORTFOLIO_HOLDING_GROUPS_COUNT

PROFILES = make_graphql_request("{app_profiles{id, user_id}}",
                                user_id=None)['data']['app_profiles']
PROFILE_IDS = {profile['user_id']: profile['id'] for profile in PROFILES}
PRICE_EPS = 1e-3


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


def get_test_portfolio_chart_filters_data():
    full_options_dict = {
        "periods": ["1d"],
        "accessTokenIds": [None, 1],
        "accountIds": [None, 7],
        "institutionIds": [None, 1],
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


def get_test_portfolio_piechart_filters_data():
    full_options_dict = {
        "accessTokenIds": [None, 1],
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

    entity_types = [
        'ticker', 'category', 'interest', 'security_type', 'collection'
    ]
    for entity_type in entity_types:
        assert abs(
            1 - piechart_sums[entity_type]['weight']) < PRICE_EPS, entity_type
        assert abs(portfolio_gains_data['absolute_gain_1d'] -
                   piechart_sums[entity_type]['absolute_daily_change']
                   ) < PRICE_EPS, entity_type
        assert abs(portfolio_gains_data['actual_value'] -
                   piechart_sums[entity_type]['absolute_value']
                   ) < PRICE_EPS, entity_type


def verify_portfolio_chart(portfolio_chart,
                           symbol_charts,
                           quantities,
                           quantities_override=[],
                           assert_message_prefix=""):
    chart_row_indexes = {}
    symbol_chart_datetimes = None
    for k, chart in symbol_charts.items():
        chart_row_indexes[k] = 0
        new_dates = set([i['datetime'] for i in chart])
        if symbol_chart_datetimes is None:
            symbol_chart_datetimes = new_dates
        else:
            symbol_chart_datetimes = symbol_chart_datetimes.union(new_dates)
            assert len(new_dates) - len(symbol_chart_datetimes) < 2

    print(quantities, quantities_override)
    portfolio_chart_index = 0
    for date in sorted(symbol_chart_datetimes):
        if datetime.datetime.now() - datetime.datetime.strptime(
                date, '%Y-%m-%dT%H:%M:%S') < datetime.timedelta(minutes=30):
            continue
        cur_quantities = quantity_override = None

        for start_date, end_date, _quantities in quantities_override:
            if start_date is not None and start_date > date:
                continue
            if end_date is not None and end_date < date:
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

            chart_row_index = chart_row_indexes[symbol]
            while chart_row_index < len(
                    symbol_charts[symbol]
            ) and symbol_charts[symbol][chart_row_index]['datetime'] < date:
                chart_row_indexes[symbol] += 1
                chart_row_index = chart_row_indexes[symbol]

            if chart_row_index < len(symbol_charts[symbol]) and symbol_charts[
                    symbol][chart_row_index]['datetime'] == date:
                expected_value += symbol_charts[symbol][chart_row_index][
                    'adjusted_close'] * quantity
                print(quantity, symbol_charts[symbol][chart_row_index])

        if portfolio_chart_index < len(portfolio_chart):
            portfolio_chart_row = portfolio_chart[portfolio_chart_index]
        else:
            portfolio_chart_row = None

        print(portfolio_chart_row)
        print()
        if portfolio_chart_row is not None and date == portfolio_chart_row[
                'datetime']:
            assert abs(
                portfolio_chart_row['adjusted_close'] - expected_value
            ) < PRICE_EPS, f"{assert_message_prefix}: wrong value on {date}: {portfolio_chart_row['adjusted_close'] }, expected {expected_value}"
            portfolio_chart_index += 1
        else:
            assert abs(
                expected_value
            ) < PRICE_EPS, f"{assert_message_prefix}: no value on {date}, expected {expected_value}"


def verify_portfolio_chart_previous_period_close(period,
                                                 previous_period_close,
                                                 quantities_override,
                                                 portfolio_chart_1y,
                                                 assert_message_prefix=""):

    if not portfolio_chart_1y:
        assert previous_period_close is None or abs(
            previous_period_close
        ) < PRICE_EPS, f"{assert_message_prefix}: wrong previous_period_close on period {period}, expected 0"
        return

    if period == '1d':
        threshold_datetime = datetime.datetime.now()
    elif period == '1w':
        threshold_datetime = datetime.datetime.now() - datetime.timedelta(
            days=7)
    elif period == '1m':
        threshold_datetime = datetime.datetime.now(
        ) - dateutil.relativedelta.relativedelta(months=1)
    elif period == '3m':
        threshold_datetime = datetime.datetime.now(
        ) - dateutil.relativedelta.relativedelta(months=3)
    else:
        raise Exception(f'{period} is not supported')
    threshold_datetime = threshold_datetime.replace(hour=0,
                                                    minute=0,
                                                    second=0,
                                                    microsecond=0)

    expected_value = None
    row_datetime = datetime.datetime.strptime(
        portfolio_chart_1y[0]['datetime'], "%Y-%m-%dT%H:%M:%S")
    for row in reversed(portfolio_chart_1y):
        row_datetime = datetime.datetime.strptime(row['datetime'],
                                                  "%Y-%m-%dT%H:%M:%S")
        if row_datetime >= threshold_datetime:
            continue

        expected_value = row['adjusted_close']
        break

    for start_date, end_date, _quantities in quantities_override:
        if start_date is not None and start_date > row_datetime.strftime(
                "%Y-%m-%dT%H:%M:%S"):
            continue
        if end_date is not None and end_date < row_datetime.strftime(
                "%Y-%m-%dT%H:%M:%S"):
            continue

        return  # TODO test complex cases

    print(period, previous_period_close, row)

    if expected_value is None:
        assert previous_period_close is None, f"{assert_message_prefix}: wrong previous_period_close for period {period}, expected {expected_value}"
    else:
        assert abs(
            previous_period_close - expected_value
        ) < PRICE_EPS, f"{assert_message_prefix}: wrong previous_period_close for period {period}, expected {expected_value}"


def verify_profile(user_id,
                   periods,
                   chart_query,
                   PROFILE_IDS,
                   charts,
                   quantities,
                   quantities_override=[]):
    profile_id = PROFILE_IDS[user_id]

    portfolio_data = make_graphql_request(chart_query, {
        "profileId": profile_id,
        "periods": ["1y"]
    },
                                          user_id=user_id)['data']
    portfolio_chart_1y = portfolio_data['get_portfolio_chart']
    for period in periods:
        previous_period_close = portfolio_data[
            'get_portfolio_chart_previous_period_close'][
                f"prev_close_{period}"]
        verify_portfolio_chart_previous_period_close(period,
                                                     previous_period_close,
                                                     quantities_override,
                                                     portfolio_chart_1y,
                                                     user_id)

    for period in periods:
        portfolio_chart = make_graphql_request(
            chart_query, {
                "profileId": profile_id,
                "periods": [period]
            },
            user_id=user_id)['data']['get_portfolio_chart']
        verify_portfolio_chart(portfolio_chart, charts[period], quantities,
                               quantities_override, period)


def get_test_portfolio_data(only_with_holdings=False):
    transaction_stats_query = "query transaction_stats($profileId: Int!) {app_profile_portfolio_transactions_aggregate(where: {profile_id: {_eq: $profileId}}) {aggregate{min{date} max{date}}}}"
    quantities = {"AAPL": 100, "AAPL240621C00225000": 200}
    quantities2 = {"AAPL": 110, "AAPL240621C00225000": 300}
    quantities3 = {"AAPL": 10, "AAPL240621C00225000": 100}

    # -- profile 1 with holdings without transactions at all
    quantities_override = [
        (None, "2022-03-10T00:00:00", {
            "AAPL": 100,
            "AAPL240621C00225000": 0
        }),
    ]
    yield ('user_id_portfolio_test_1', quantities, quantities_override)

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
         ['aggregate']['max']['date'], quantities2),
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
         ['aggregate']['max']['date'], quantities2),
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
         ['aggregate']['max']['date'], quantities2),
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
         ['aggregate']['max']['date'], quantities2),
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

    if only_with_holdings:
        return

    quantities = {"AAPL": 0, "AAPL240621C00225000": 0}

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
                 ['aggregate']['max']['date'], quantities3),
            ]
            yield ('user_id_portfolio_test_' + str(i), quantities,
                   quantities_override)


@pytest.mark.parametrize("user_id,quantities,quantities_override",
                         get_test_portfolio_data())
def test_portfolio_chart_data(user_id, quantities, quantities_override):
    periods = ["1d", "1w", "1m", "3m"]

    query = 'query chart($period: String!, $symbol: String!) { chart(where: {symbol: {_eq: $symbol}, period: {_eq: $period}}, order_by: {datetime: asc}) { symbol datetime period open high low close adjusted_close volume } }'
    charts = {
        period: {
            "AAPL":
            make_graphql_request(query, {
                "period": period,
                "symbol": "AAPL"
            })['data']['chart'],
            "AAPL240621C00225000":
            make_graphql_request(query, {
                "period": period,
                "symbol": "AAPL240621C00225000"
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


@pytest.mark.parametrize("user_id,quantities,quantities_override",
                         get_test_portfolio_data(only_with_holdings=True))
def test_portfolio_holdings_data(user_id, quantities, quantities_override):
    query = 'query ticker_metrics($symbol: String!) { ticker_metrics(where: {symbol: {_eq: $symbol}}) { price_change_1m price_change_1w price_change_1y price_change_3m price_change_5y price_change_all } ticker_realtime_metrics(where: {symbol: {_eq: $symbol}}) { actual_price relative_daily_change } }'
    metrics = {
        "AAPL":
        make_graphql_request(query, {"symbol": "AAPL"})['data'],
        "AAPL240621C00225000":
        make_graphql_request(query, {"symbol": "AAPL240621C00225000"})['data'],
    }

    # flatten metrics dict
    metrics = {
        k: {
            **i['ticker_realtime_metrics'][0],
            **i['ticker_metrics'][0]
        }
        for k, i in metrics.items()
    }

    query_file = os.path.join(os.path.dirname(__file__),
                              'queries/GetPortfolioHoldings.graphql')
    with open(query_file, 'r') as f:
        query = f.read()

    profile_id = PROFILE_IDS[user_id]
    data = make_graphql_request(query, {"profileId": profile_id},
                                user_id=user_id)['data']
    portfolio_gains = data['portfolio_gains'][0]
    profile_holding_groups = data['profile_holding_groups']
    profile_chart_latest_point = data['get_portfolio_chart'][-1]

    periods_mapping = {
        "gain_1d": "relative_daily_change",
        "gain_1w": "price_change_1w",
        "gain_1m": "price_change_1m",
        "gain_3m": "price_change_3m",
        "gain_1y": "price_change_1y",
        "gain_5y": "price_change_5y",
        "gain_total": "price_change_all"
    }

    actual_portfolio_value = 0
    for symbol, quantity in quantities.items():
        actual_portfolio_value += metrics[symbol]['actual_price'] * quantity
    assert abs(portfolio_gains['actual_value'] -
               actual_portfolio_value) < PRICE_EPS
    assert abs(profile_chart_latest_point['adjusted_close'] -
               actual_portfolio_value) < PRICE_EPS

    for portfolio_key, metrics_key in periods_mapping.items():
        relative_portfolio_key = f'relative_{portfolio_key}'
        absolute_portfolio_key = f'absolute_{portfolio_key}'
        absolute_symbol_price_change = {
            symbol: symbol_metrics['actual_price'] *
            (1 - 1 / (1 + symbol_metrics[metrics_key]))
            for symbol, symbol_metrics in metrics.items()
        }

        expected_absolute_gain = 0
        period_start_portfolio_value = 0
        for symbol, value in absolute_symbol_price_change.items():
            expected_absolute_gain += value * quantities[symbol]
            period_start_portfolio_value += (metrics[symbol]['actual_price'] -
                                             value) * quantities[symbol]
        expected_relative_gain = actual_portfolio_value / period_start_portfolio_value - 1

        assert abs(portfolio_gains[relative_portfolio_key] -
                   expected_relative_gain) < PRICE_EPS
        assert abs(portfolio_gains[absolute_portfolio_key] -
                   expected_absolute_gain) < PRICE_EPS

        for holding_group in profile_holding_groups:
            symbol = holding_group['details']['ticker_symbol']
            gains = holding_group['gains']

            if relative_portfolio_key in [
                    'relative_gain_1d', 'relative_gain_total'
            ]:
                assert abs(holding_group['details'][relative_portfolio_key] -
                           metrics[symbol][metrics_key]) < PRICE_EPS
            assert abs(gains[relative_portfolio_key] -
                       metrics[symbol][metrics_key]) < PRICE_EPS
            assert abs(gains[absolute_portfolio_key] -
                       absolute_symbol_price_change[symbol]) < PRICE_EPS

            for holding in holding_group['holdings']:
                symbol = holding['holding_details']['ticker_symbol']
                holding_type = holding['type']
                assert holding_type in [
                    'equity',
                    'derivative',
                ], f'{holding_type} holdings are not supported'
                gains = holding['gains']

                if relative_portfolio_key in [
                        'relative_gain_1d', 'relative_gain_total'
                ]:
                    assert abs(
                        holding['holding_details'][relative_portfolio_key] -
                        metrics[symbol][metrics_key]
                    ) < PRICE_EPS, relative_portfolio_key
                assert abs(gains[relative_portfolio_key] - metrics[symbol]
                           [metrics_key]) < PRICE_EPS, relative_portfolio_key
                assert abs(gains[absolute_portfolio_key] -
                           absolute_symbol_price_change[symbol]
                           ) < PRICE_EPS, absolute_portfolio_key

    seen_symbols = set()
    for holding_group in profile_holding_groups:
        seen_symbols.add(symbol)

        symbol = holding_group['details']['ticker_symbol']
        holding_group_value = metrics[symbol]['actual_price'] * quantities[
            symbol]
        assert abs(gains['actual_value'] - holding_group_value) < PRICE_EPS
        assert abs(gains['value_to_portfolio_value'] -
                   holding_group_value / actual_portfolio_value) < PRICE_EPS

        for holding in holding_group['holdings']:
            holding_type = holding['type']
            assert holding_type in [
                'equity',
                'derivative',
            ], f'{holding_type} holdings are not supported'
            holding_value = metrics[symbol]['actual_price'] * quantities[symbol]
            assert abs(gains['actual_value'] - holding_value) < PRICE_EPS
            assert abs(gains['value_to_portfolio_value'] -
                       holding_value / actual_portfolio_value) < PRICE_EPS

    assert seen_symbols <= set(metrics.keys())
