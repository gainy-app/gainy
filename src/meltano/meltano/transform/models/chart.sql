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
             join {{ ref('base_tickers') }} on base_tickers.symbol = historical_prices_aggregated.symbol
             left join latest_open_trading_session
                       on (latest_open_trading_session.exchange_name = base_tickers.exchange_canonical or
                           (base_tickers.exchange_canonical is null and
                            latest_open_trading_session.country_name = base_tickers.country_name))
                      and latest_open_trading_session.date = historical_prices_aggregated.datetime::date
    where period = '3min'
      and (historical_prices_aggregated.datetime between latest_open_trading_session.open_at and latest_open_trading_session.close_at
        or (base_tickers.type = 'crypto' and historical_prices_aggregated.datetime > now() - interval '1 day'))
)

union all

(
    with week_trading_sessions as (
        select *
        from {{ ref('exchange_schedule') }}
        where open_at between now() - interval '1 week' and now()
    )
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
             join {{ ref('base_tickers') }} on base_tickers.symbol = historical_prices_aggregated.symbol
             left join week_trading_sessions
                       on (week_trading_sessions.exchange_name = base_tickers.exchange_canonical or
                           (base_tickers.exchange_canonical is null and
                            week_trading_sessions.country_name = base_tickers.country_name))
                      and week_trading_sessions.date = historical_prices_aggregated.datetime::date
    where period = '15min'
      and (historical_prices_aggregated.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at
       or (base_tickers.type = 'crypto' and historical_prices_aggregated.datetime > now() - interval '7 days'))
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
             join {{ ref('base_tickers') }} on base_tickers.symbol = historical_prices_aggregated.symbol
             left join latest_open_trading_session
                       on (latest_open_trading_session.exchange_name = base_tickers.exchange_canonical or
                           (base_tickers.exchange_canonical is null and
                            latest_open_trading_session.country_name = base_tickers.country_name))
    where period = '1d'
      and historical_prices_aggregated.datetime >= coalesce(latest_open_trading_session.date, now()) - interval '1 month'
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
             join {{ ref('base_tickers') }} on base_tickers.symbol = historical_prices_aggregated.symbol
             left join latest_open_trading_session
                       on (latest_open_trading_session.exchange_name = base_tickers.exchange_canonical or
                           (base_tickers.exchange_canonical is null and
                            latest_open_trading_session.country_name = base_tickers.country_name))
    where period = '1d'
      and historical_prices_aggregated.datetime >= coalesce(latest_open_trading_session.date, now()) - interval '3 month'
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
             join {{ ref('base_tickers') }} on base_tickers.symbol = historical_prices_aggregated.symbol
             left join latest_open_trading_session
                       on (latest_open_trading_session.exchange_name = base_tickers.exchange_canonical or
                           (base_tickers.exchange_canonical is null and
                            latest_open_trading_session.country_name = base_tickers.country_name))
    where period = '1d'
      and historical_prices_aggregated.datetime >= coalesce(latest_open_trading_session.date, now()) - interval '1 year'
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
             join {{ ref('base_tickers') }} on base_tickers.symbol = historical_prices_aggregated.symbol
             left join latest_open_trading_session
                       on (latest_open_trading_session.exchange_name = base_tickers.exchange_canonical or
                           (base_tickers.exchange_canonical is null and
                            latest_open_trading_session.country_name = base_tickers.country_name))
    where period = '1w'
      and historical_prices_aggregated.datetime >= coalesce(latest_open_trading_session.date, now()) - interval '5 year'
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