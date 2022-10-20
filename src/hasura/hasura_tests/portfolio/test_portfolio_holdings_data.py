import logging

import datetime

import dateutil.parser
import dateutil.relativedelta
import os
import pytest
from hasura_tests.common import make_graphql_request, PRICE_EPS
from hasura_tests.portfolio.common import get_test_portfolio_data, PROFILE_IDS


@pytest.mark.parametrize("user_id,quantities,quantities_override",
                         get_test_portfolio_data(only_with_holdings=True))
def test_portfolio_holdings_data(user_id, quantities, quantities_override):
    query = '''query metrics($symbol: String!) { 
        ticker_metrics(where: {symbol: {_eq: $symbol}}) { price_change_1m price_change_1w price_change_1y price_change_3m price_change_5y price_change_all } 
        ticker_realtime_metrics(where: {symbol: {_eq: $symbol}}) { actual_price relative_daily_change }
        chart(where: {symbol: {_eq: $symbol}, period: {_eq: "1m"}}, distinct_on: symbol, order_by: {symbol: asc, datetime: desc}) { datetime } 
    }'''
    metrics = {
        "AAPL":
        make_graphql_request(query, {"symbol": "AAPL"})['data'],
        "AAPL240621C00225000":
        make_graphql_request(query, {"symbol": "AAPL240621C00225000"})['data'],
    }

    last_trading_day_date = max(i['chart'][0]['datetime'] for i in metrics.values())
    last_trading_day_date = dateutil.parser.parse(last_trading_day_date)

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
        "gain_1d": ("relative_daily_change", datetime.timedelta(days=0)),
        "gain_1w": ("price_change_1w", datetime.timedelta(days=7)),
        "gain_1m": ("price_change_1m", dateutil.relativedelta.relativedelta(months=1)),
        "gain_3m": ("price_change_3m", dateutil.relativedelta.relativedelta(months=3)),
        "gain_1y": ("price_change_1y", dateutil.relativedelta.relativedelta(years=1)),
        "gain_5y": ("price_change_5y", dateutil.relativedelta.relativedelta(years=5)),
        "gain_total": ("price_change_all", None),
    }

    actual_portfolio_value = 0
    for symbol, quantity in quantities.items():
        actual_portfolio_value += metrics[symbol]['actual_price'] * quantity
    assert abs(portfolio_gains['actual_value'] -
               actual_portfolio_value) < PRICE_EPS
    assert abs(profile_chart_latest_point['adjusted_close'] -
               actual_portfolio_value) < PRICE_EPS

    for portfolio_key, (metrics_key, timedelta) in periods_mapping.items():
        print(portfolio_key, metrics_key)
        relative_portfolio_key = f'relative_{portfolio_key}'
        absolute_portfolio_key = f'absolute_{portfolio_key}'
        absolute_symbol_price_change = {
            symbol: symbol_metrics['actual_price'] *
            (1 - 1 / (1 + symbol_metrics[metrics_key]))
            for symbol, symbol_metrics in metrics.items()
        }

        holding_group_absolute_gain_sum = 0
        for holding_group in profile_holding_groups:
            holding_absolute_gain_sum = 0
            for holding in holding_group['holdings']:
                symbol = holding['holding_details']['ticker_symbol']
                holding_type = holding['type']
                quantity = holding['quantity']
                if holding['type'] == 'derivative':
                    quantity *= 100

                assert holding_type in [
                    'equity',
                    'derivative',
                ], f'{holding_type} holdings are not supported'
                gains = holding['gains']
                holding_absolute_gain_sum += gains[absolute_portfolio_key]

                purchase_date = holding['holding_details']['purchase_date']
                if purchase_date:
                    purchase_date = dateutil.parser.parse(purchase_date) if purchase_date else None
                    if timedelta is None or purchase_date >= last_trading_day_date - timedelta:
                        logging.info(f'Skipping holding {symbol}')
                        continue

                if relative_portfolio_key in [
                        'relative_gain_1d', 'relative_gain_total'
                ]:
                    assert abs(
                        holding['holding_details'][relative_portfolio_key] -
                        metrics[symbol][metrics_key]
                    ) < PRICE_EPS, (relative_portfolio_key, symbol)
                assert abs(gains[relative_portfolio_key] - metrics[symbol]
                           [metrics_key]) < PRICE_EPS, (relative_portfolio_key, symbol)
                assert abs(gains[absolute_portfolio_key] -
                           quantity * absolute_symbol_price_change[symbol]
                           ) < PRICE_EPS, (absolute_portfolio_key, symbol)

            symbol = holding_group['details']['ticker_symbol']
            gains = holding_group['gains']
            actual_value = gains['actual_value']
            prev_value = actual_value - holding_absolute_gain_sum
            expected_relative_gain = holding_absolute_gain_sum / prev_value
            if relative_portfolio_key in [
                    'relative_gain_1d', 'relative_gain_total'
            ]:
                assert abs(holding_group['details'][relative_portfolio_key] -
                           expected_relative_gain) < PRICE_EPS, (absolute_portfolio_key, symbol)
            assert abs(gains[relative_portfolio_key] -
                       expected_relative_gain) < PRICE_EPS, (absolute_portfolio_key, symbol)
            assert abs(gains[absolute_portfolio_key] -
                       holding_absolute_gain_sum) < PRICE_EPS, (absolute_portfolio_key, symbol)
            holding_group_absolute_gain_sum += gains[absolute_portfolio_key]

        actual_value = portfolio_gains['actual_value']
        prev_value = actual_value - holding_group_absolute_gain_sum
        expected_relative_gain = holding_group_absolute_gain_sum / prev_value
        assert abs(portfolio_gains[relative_portfolio_key] -
                   expected_relative_gain) < PRICE_EPS
        assert abs(portfolio_gains[absolute_portfolio_key] -
                   holding_group_absolute_gain_sum) < PRICE_EPS

    seen_symbols = set()
    for holding_group in profile_holding_groups:
        holdings_value_sum = 0

        for holding in holding_group['holdings']:
            gains = holding['gains']
            symbol = holding['holding_details']['ticker_symbol']
            holding_type = holding['type']
            assert holding_type in [
                'equity',
                'derivative',
            ], f'{holding_type} holdings are not supported'
            holding_value = metrics[symbol]['actual_price'] * quantities[symbol]
            assert abs(gains['actual_value'] - holding_value) < PRICE_EPS
            assert abs(gains['value_to_portfolio_value'] -
                       holding_value / actual_portfolio_value) < PRICE_EPS
            holdings_value_sum += gains['actual_value']

        symbol = holding_group['details']['ticker_symbol']
        gains = holding_group['gains']
        seen_symbols.add(symbol)

        assert abs(gains['actual_value'] - holdings_value_sum) < PRICE_EPS
        assert abs(gains['value_to_portfolio_value'] -
                   holdings_value_sum / actual_portfolio_value) < PRICE_EPS

    assert seen_symbols <= set(metrics.keys())
