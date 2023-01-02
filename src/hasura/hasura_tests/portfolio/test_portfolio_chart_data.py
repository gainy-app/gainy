import datetime
import dateutil.parser
import numbers
import os
import pytest

from hasura_tests.common import make_graphql_request, PRICE_EPS
from hasura_tests.portfolio.common import PROFILE_IDS

DATETIME_ISO8601_FORMAT = '%Y-%m-%dT%H:%M:%S'


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

    print(quantities, quantities_override,
          list(sorted(symbol_chart_datetimes)))
    portfolio_chart_index = 0
    for date in sorted(symbol_chart_datetimes):
        if datetime.datetime.now() - datetime.datetime.strptime(
                date, DATETIME_ISO8601_FORMAT) < datetime.timedelta(
                    minutes=30):
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
                                                 threshold_datetime,
                                                 quantities_override,
                                                 portfolio_chart_1y,
                                                 assert_message_prefix=""):

    if not portfolio_chart_1y:
        assert previous_period_close is None or abs(
            previous_period_close
        ) < PRICE_EPS, f"{assert_message_prefix}: wrong previous_period_close on period {period}, expected 0"
        return

    threshold_datetime = threshold_datetime.replace(hour=0,
                                                    minute=0,
                                                    second=0,
                                                    microsecond=0)

    expected_value = None
    row_datetime = datetime.datetime.strptime(
        portfolio_chart_1y[0]['datetime'], DATETIME_ISO8601_FORMAT)
    for row in reversed(portfolio_chart_1y):
        row_datetime = datetime.datetime.strptime(row['datetime'],
                                                  DATETIME_ISO8601_FORMAT)
        if row_datetime >= threshold_datetime:
            continue

        expected_value = row['adjusted_close']
        break

    for start_date, end_date, _quantities in quantities_override:
        if start_date is not None and start_date > row_datetime.strftime(
                DATETIME_ISO8601_FORMAT):
            continue
        if end_date is not None and end_date < row_datetime.strftime(
                DATETIME_ISO8601_FORMAT):
            continue

        return  # TODO test complex cases

    print(period, threshold_datetime, previous_period_close, row)

    if expected_value is None:
        assert previous_period_close is None, f"{assert_message_prefix}: wrong previous_period_close for period {period}, expected {expected_value}"
    else:
        assert previous_period_close is not None, f"{assert_message_prefix}: wrong previous_period_close for period {period}, expected {expected_value}"
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
        print(profile_id, period)
        portfolio_chart = make_graphql_request(
            chart_query, {
                "profileId": profile_id,
                "periods": [period]
            },
            user_id=user_id)['data']['get_portfolio_chart']
        verify_portfolio_chart(portfolio_chart, charts[period], quantities,
                               quantities_override, period)

        previous_period_close = portfolio_data[
            'get_portfolio_chart_previous_period_close'][
                f"prev_close_{period}"]

        if not portfolio_chart:
            # TODO since the chart is filtered, we don't know the first date of the weekly chart,
            # so we can't reliably check previous_period_close
            # assert not previous_period_close
            continue

        threshold_datetime = min(i['datetime'] for i in portfolio_chart)
        threshold_datetime = dateutil.parser.parse(threshold_datetime)
        verify_portfolio_chart_previous_period_close(
            period, previous_period_close, threshold_datetime,
            quantities_override, portfolio_chart_1y, user_id)


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
