{{
  config(
    materialized = "view",
  )
}}

with latest_open_trading_session as (
    select distinct on (exchange_name) *
    from exchange_schedule
    where open_at <= now()
    order by exchange_name, date desc
)

    (
        select historical_prices_aggregated.symbol,
               historical_prices_aggregated.datetime,
               '1d'::varchar as period,
               historical_prices_aggregated.open,
               historical_prices_aggregated.high,
               historical_prices_aggregated.low,
               historical_prices_aggregated.close,
               historical_prices_aggregated.adjusted_close,
               historical_prices_aggregated.volume
        from {{ ref('historical_prices_aggregated') }}
                 join {{ ref('tickers') }} on tickers.symbol = historical_prices_aggregated.symbol
                 join latest_open_trading_session
                      on latest_open_trading_session.exchange_name = (string_to_array(tickers.exchange, ' '))[1]
                          and latest_open_trading_session.date = historical_prices_aggregated.datetime::date
        where period = '15min'
          and historical_prices_aggregated.datetime between latest_open_trading_session.open_at and latest_open_trading_session.close_at
    )

union all

(
    select historical_prices_aggregated.symbol,
           historical_prices_aggregated.datetime,
           '1w'::varchar as period,
           historical_prices_aggregated.open,
           historical_prices_aggregated.high,
           historical_prices_aggregated.low,
           historical_prices_aggregated.close,
           historical_prices_aggregated.adjusted_close,
           historical_prices_aggregated.volume
    from {{ ref('historical_prices_aggregated') }}
             join {{ ref('tickers') }} on tickers.symbol = historical_prices_aggregated.symbol
             join latest_open_trading_session
                  on latest_open_trading_session.exchange_name = (string_to_array(tickers.exchange, ' '))[1]
    where period = '1d'
      and ((historical_prices_aggregated.datetime > latest_open_trading_session.date - interval '1 week' and latest_open_trading_session.close_at < now())
       or (historical_prices_aggregated.datetime >= latest_open_trading_session.date - interval '1 week' and latest_open_trading_session.close_at >= now()))
)

union all

(
    select historical_prices_aggregated.symbol,
           historical_prices_aggregated.datetime,
           '1m'::varchar as period,
           historical_prices_aggregated.open,
           historical_prices_aggregated.high,
           historical_prices_aggregated.low,
           historical_prices_aggregated.close,
           historical_prices_aggregated.adjusted_close,
           historical_prices_aggregated.volume
    from {{ ref('historical_prices_aggregated') }}
             join {{ ref('tickers') }} on tickers.symbol = historical_prices_aggregated.symbol
             join latest_open_trading_session
                  on latest_open_trading_session.exchange_name = (string_to_array(tickers.exchange, ' '))[1]
    where period = '1d'
      and historical_prices_aggregated.datetime >= latest_open_trading_session.date - interval '1 month'
)

union all

(
    select historical_prices_aggregated.symbol,
           historical_prices_aggregated.datetime,
           '3m'::varchar as period,
           historical_prices_aggregated.open,
           historical_prices_aggregated.high,
           historical_prices_aggregated.low,
           historical_prices_aggregated.close,
           historical_prices_aggregated.adjusted_close,
           historical_prices_aggregated.volume
    from {{ ref('historical_prices_aggregated') }}
             join {{ ref('tickers') }} on tickers.symbol = historical_prices_aggregated.symbol
             join latest_open_trading_session
                  on latest_open_trading_session.exchange_name = (string_to_array(tickers.exchange, ' '))[1]
    where period = '1w'
      and historical_prices_aggregated.datetime >= latest_open_trading_session.date - interval '3 month'
)

union all

(
    select historical_prices_aggregated.symbol,
           historical_prices_aggregated.datetime,
           '1y'::varchar as period,
           historical_prices_aggregated.open,
           historical_prices_aggregated.high,
           historical_prices_aggregated.low,
           historical_prices_aggregated.close,
           historical_prices_aggregated.adjusted_close,
           historical_prices_aggregated.volume
    from {{ ref('historical_prices_aggregated') }}
             join {{ ref('tickers') }} on tickers.symbol = historical_prices_aggregated.symbol
             join latest_open_trading_session
                  on latest_open_trading_session.exchange_name = (string_to_array(tickers.exchange, ' '))[1]
    where period = '1w'
      and historical_prices_aggregated.datetime >= latest_open_trading_session.date - interval '1 year'
)

union all

(
    select historical_prices_aggregated.symbol,
           historical_prices_aggregated.datetime,
           '5y'::varchar as period,
           historical_prices_aggregated.open,
           historical_prices_aggregated.high,
           historical_prices_aggregated.low,
           historical_prices_aggregated.close,
           historical_prices_aggregated.adjusted_close,
           historical_prices_aggregated.volume
    from {{ ref('historical_prices_aggregated') }}
             join {{ ref('tickers') }} on tickers.symbol = historical_prices_aggregated.symbol
             join latest_open_trading_session
                  on latest_open_trading_session.exchange_name = (string_to_array(tickers.exchange, ' '))[1]
    where period = '1m'
      and historical_prices_aggregated.datetime >= latest_open_trading_session.date - interval '5 year'
)

union all

(
    select historical_prices_aggregated.symbol,
           historical_prices_aggregated.datetime,
           'all'::varchar as period,
           historical_prices_aggregated.open,
           historical_prices_aggregated.high,
           historical_prices_aggregated.low,
           historical_prices_aggregated.close,
           historical_prices_aggregated.adjusted_close,
           historical_prices_aggregated.volume
    from {{ ref('historical_prices_aggregated') }}
    where period = '1m'
)