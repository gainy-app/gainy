{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}

with latest_open_trading_session as (
    select distinct on (exchange_name) *
    from {{ ref('exchange_schedule') }}
    where open_at <= now()
    order by exchange_name, date desc
)

(
    select historical_prices_aggregated_3min.symbol,
           historical_prices_aggregated_3min.datetime,
           '1d'::varchar as period,
           historical_prices_aggregated_3min.open,
           historical_prices_aggregated_3min.high,
           historical_prices_aggregated_3min.low,
           historical_prices_aggregated_3min.close,
           historical_prices_aggregated_3min.adjusted_close,
           historical_prices_aggregated_3min.volume
    from {{ ref('historical_prices_aggregated_3min') }}
             left join {{ ref('ticker_options_monitored') }}
                  on ticker_options_monitored.contract_name = historical_prices_aggregated_3min.symbol
             join {{ ref('base_tickers') }}
                  on base_tickers.symbol = historical_prices_aggregated_3min.symbol
                      or base_tickers.symbol = ticker_options_monitored.symbol
             left join latest_open_trading_session
                       on (latest_open_trading_session.exchange_name = base_tickers.exchange_canonical or
                           (base_tickers.exchange_canonical is null and
                            latest_open_trading_session.country_name = base_tickers.country_name))
                      and latest_open_trading_session.date = historical_prices_aggregated_3min.datetime::date
    where (historical_prices_aggregated_3min.datetime between latest_open_trading_session.open_at and latest_open_trading_session.close_at - interval '3 minutes'
       or (base_tickers.type = 'crypto' and historical_prices_aggregated_3min.datetime > now() - interval '1 day'))
)

union all

(
    with week_trading_sessions as (
        select *
        from {{ ref('exchange_schedule') }}
        where open_at between now() - interval '1 week' and now()
    )
    select historical_prices_aggregated_15min.symbol,
           historical_prices_aggregated_15min.datetime,
           '1w'::varchar as period,
           historical_prices_aggregated_15min.open,
           historical_prices_aggregated_15min.high,
           historical_prices_aggregated_15min.low,
           historical_prices_aggregated_15min.close,
           historical_prices_aggregated_15min.adjusted_close,
           historical_prices_aggregated_15min.volume
    from {{ ref('historical_prices_aggregated_15min') }}
             left join {{ ref('ticker_options_monitored') }}
                  on ticker_options_monitored.contract_name = historical_prices_aggregated_15min.symbol
             join {{ ref('base_tickers') }}
                  on base_tickers.symbol = historical_prices_aggregated_15min.symbol
                      or base_tickers.symbol = ticker_options_monitored.symbol
             left join week_trading_sessions
                       on (week_trading_sessions.exchange_name = base_tickers.exchange_canonical or
                           (base_tickers.exchange_canonical is null and
                            week_trading_sessions.country_name = base_tickers.country_name))
                      and week_trading_sessions.date = historical_prices_aggregated_15min.datetime::date
    where (historical_prices_aggregated_15min.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at - interval '15 minutes'
       or (base_tickers.type = 'crypto' and historical_prices_aggregated_15min.datetime > now() - interval '7 days'))
)

union all

(
    select historical_prices_aggregated_1d.symbol,
           historical_prices_aggregated_1d.datetime,
           '1m'::varchar as period,
           historical_prices_aggregated_1d.open,
           historical_prices_aggregated_1d.high,
           historical_prices_aggregated_1d.low,
           historical_prices_aggregated_1d.close,
           historical_prices_aggregated_1d.adjusted_close,
           historical_prices_aggregated_1d.volume
    from {{ ref('historical_prices_aggregated_1d') }}
             left join {{ ref('ticker_options_monitored') }}
                  on ticker_options_monitored.contract_name = historical_prices_aggregated_1d.symbol
             join {{ ref('base_tickers') }}
                  on base_tickers.symbol = historical_prices_aggregated_1d.symbol
                      or base_tickers.symbol = ticker_options_monitored.symbol
             left join latest_open_trading_session
                       on (latest_open_trading_session.exchange_name = base_tickers.exchange_canonical or
                           (base_tickers.exchange_canonical is null and
                            latest_open_trading_session.country_name = base_tickers.country_name))
    where historical_prices_aggregated_1d.datetime >= coalesce(latest_open_trading_session.date, now()) - interval '1 month'
)

union all

(
    select historical_prices_aggregated_1d.symbol,
           historical_prices_aggregated_1d.datetime,
           '3m'::varchar as period,
           historical_prices_aggregated_1d.open,
           historical_prices_aggregated_1d.high,
           historical_prices_aggregated_1d.low,
           historical_prices_aggregated_1d.close,
           historical_prices_aggregated_1d.adjusted_close,
           historical_prices_aggregated_1d.volume
    from {{ ref('historical_prices_aggregated_1d') }}
             left join {{ ref('ticker_options_monitored') }}
                  on ticker_options_monitored.contract_name = historical_prices_aggregated_1d.symbol
             join {{ ref('base_tickers') }}
                  on base_tickers.symbol = historical_prices_aggregated_1d.symbol
                      or base_tickers.symbol = ticker_options_monitored.symbol
             left join latest_open_trading_session
                       on (latest_open_trading_session.exchange_name = base_tickers.exchange_canonical or
                           (base_tickers.exchange_canonical is null and
                            latest_open_trading_session.country_name = base_tickers.country_name))
    where historical_prices_aggregated_1d.datetime >= coalesce(latest_open_trading_session.date, now()) - interval '3 month'
)

union all

(
    select historical_prices_aggregated_1d.symbol,
           historical_prices_aggregated_1d.datetime,
           '1y'::varchar as period,
           historical_prices_aggregated_1d.open,
           historical_prices_aggregated_1d.high,
           historical_prices_aggregated_1d.low,
           historical_prices_aggregated_1d.close,
           historical_prices_aggregated_1d.adjusted_close,
           historical_prices_aggregated_1d.volume
    from {{ ref('historical_prices_aggregated_1d') }}
             left join {{ ref('ticker_options_monitored') }}
                  on ticker_options_monitored.contract_name = historical_prices_aggregated_1d.symbol
             join {{ ref('base_tickers') }}
                  on base_tickers.symbol = historical_prices_aggregated_1d.symbol
                      or base_tickers.symbol = ticker_options_monitored.symbol
             left join latest_open_trading_session
                       on (latest_open_trading_session.exchange_name = base_tickers.exchange_canonical or
                           (base_tickers.exchange_canonical is null and
                            latest_open_trading_session.country_name = base_tickers.country_name))
    where historical_prices_aggregated_1d.datetime >= coalesce(latest_open_trading_session.date, now()) - interval '1 year'
)

union all

(
    select historical_prices_aggregated_1w.symbol,
           historical_prices_aggregated_1w.datetime,
           '5y'::varchar as period,
           historical_prices_aggregated_1w.open,
           historical_prices_aggregated_1w.high,
           historical_prices_aggregated_1w.low,
           historical_prices_aggregated_1w.close,
           historical_prices_aggregated_1w.adjusted_close,
           historical_prices_aggregated_1w.volume
    from {{ ref('historical_prices_aggregated_1w') }}
             left join {{ ref('ticker_options_monitored') }}
                  on ticker_options_monitored.contract_name = historical_prices_aggregated_1w.symbol
             join {{ ref('base_tickers') }}
                  on base_tickers.symbol = historical_prices_aggregated_1w.symbol
                      or base_tickers.symbol = ticker_options_monitored.symbol
             left join latest_open_trading_session
                       on (latest_open_trading_session.exchange_name = base_tickers.exchange_canonical or
                           (base_tickers.exchange_canonical is null and
                            latest_open_trading_session.country_name = base_tickers.country_name))
    where historical_prices_aggregated_1w.datetime >= coalesce(latest_open_trading_session.date, now()) - interval '5 year'
)

union all

(
    select symbol,
           datetime,
           'all'::varchar as period,
           open,
           high,
           low,
           close,
           adjusted_close,
           volume
    from {{ ref('historical_prices_aggregated_1m') }}
)
