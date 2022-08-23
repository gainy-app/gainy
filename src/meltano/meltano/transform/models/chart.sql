{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}


(
    select historical_prices_aggregated_3min.symbol,
           historical_prices_aggregated_3min.datetime,
           historical_prices_aggregated_3min.datetime + interval '3 minutes' as close_datetime,
           '1d'::varchar as period,
           historical_prices_aggregated_3min.open,
           historical_prices_aggregated_3min.high,
           historical_prices_aggregated_3min.low,
           historical_prices_aggregated_3min.close,
           historical_prices_aggregated_3min.adjusted_close,
           historical_prices_aggregated_3min.volume
    from {{ ref('historical_prices_aggregated_3min') }}
             join {{ ref('week_trading_sessions') }} using (symbol)
    where week_trading_sessions.index = 0
      and historical_prices_aggregated_3min.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at - interval '3 minutes'
)

union all

(
    select historical_prices_aggregated_15min.symbol,
           historical_prices_aggregated_15min.datetime,
           historical_prices_aggregated_15min.datetime + interval '15 minutes' as close_datetime,
           '1w'::varchar as period,
           historical_prices_aggregated_15min.open,
           historical_prices_aggregated_15min.high,
           historical_prices_aggregated_15min.low,
           historical_prices_aggregated_15min.close,
           historical_prices_aggregated_15min.adjusted_close,
           historical_prices_aggregated_15min.volume
    from {{ ref('historical_prices_aggregated_15min') }}
             join {{ ref('week_trading_sessions') }} using (symbol)
    where historical_prices_aggregated_15min.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at - interval '15 minutes'
)

union all

(
    with latest_open_trading_session as
             (
                 select symbol, max(date) as date
                 from {{ ref('week_trading_sessions') }}
                 where index = 0
                 group by symbol
             )
    select historical_prices_aggregated_1d.symbol,
           historical_prices_aggregated_1d.datetime,
           historical_prices_aggregated_1d.datetime + interval '1 day' as close_datetime,
           '1m'::varchar as period,
           historical_prices_aggregated_1d.open,
           historical_prices_aggregated_1d.high,
           historical_prices_aggregated_1d.low,
           historical_prices_aggregated_1d.close,
           historical_prices_aggregated_1d.adjusted_close,
           historical_prices_aggregated_1d.volume
    from {{ ref('historical_prices_aggregated_1d') }}
             join latest_open_trading_session using (symbol)
    where historical_prices_aggregated_1d.datetime >= coalesce(latest_open_trading_session.date, now()) - interval '1 month'
)

union all

(
    with latest_open_trading_session as
             (
                 select symbol, max(date) as date
                 from {{ ref('week_trading_sessions') }}
                 where index = 0
                 group by symbol
             )
    select historical_prices_aggregated_1d.symbol,
           historical_prices_aggregated_1d.datetime,
           historical_prices_aggregated_1d.datetime + interval '1 day' as close_datetime,
           '3m'::varchar as period,
           historical_prices_aggregated_1d.open,
           historical_prices_aggregated_1d.high,
           historical_prices_aggregated_1d.low,
           historical_prices_aggregated_1d.close,
           historical_prices_aggregated_1d.adjusted_close,
           historical_prices_aggregated_1d.volume
    from {{ ref('historical_prices_aggregated_1d') }}
             join latest_open_trading_session using (symbol)
    where historical_prices_aggregated_1d.datetime >= coalesce(latest_open_trading_session.date, now()) - interval '3 month'
)

union all

(
    with latest_open_trading_session as
             (
                 select symbol, max(date) as date
                 from {{ ref('week_trading_sessions') }}
                 where index = 0
                 group by symbol
             )
    select historical_prices_aggregated_1d.symbol,
           historical_prices_aggregated_1d.datetime,
           historical_prices_aggregated_1d.datetime + interval '1 day' as close_datetime,
           '1y'::varchar as period,
           historical_prices_aggregated_1d.open,
           historical_prices_aggregated_1d.high,
           historical_prices_aggregated_1d.low,
           historical_prices_aggregated_1d.close,
           historical_prices_aggregated_1d.adjusted_close,
           historical_prices_aggregated_1d.volume
    from {{ ref('historical_prices_aggregated_1d') }}
             join latest_open_trading_session using (symbol)
    where historical_prices_aggregated_1d.datetime >= coalesce(latest_open_trading_session.date, now()) - interval '1 year'
)

union all

(
    with latest_open_trading_session as
             (
                 select symbol, max(date) as date
                 from {{ ref('week_trading_sessions') }}
                 where index = 0
                 group by symbol
             )
    select historical_prices_aggregated_1w.symbol,
           historical_prices_aggregated_1w.datetime,
           historical_prices_aggregated_1w.datetime + interval '1 week' as close_datetime,
           '5y'::varchar as period,
           historical_prices_aggregated_1w.open,
           historical_prices_aggregated_1w.high,
           historical_prices_aggregated_1w.low,
           historical_prices_aggregated_1w.close,
           historical_prices_aggregated_1w.adjusted_close,
           historical_prices_aggregated_1w.volume
    from {{ ref('historical_prices_aggregated_1w') }}
             join latest_open_trading_session using (symbol)
    where historical_prices_aggregated_1w.datetime >= coalesce(latest_open_trading_session.date, now()) - interval '5 year'
)

union all

(
    select symbol,
           datetime,
           datetime + interval '1 month' as close_datetime,
           'all'::varchar                as period,
           open,
           high,
           low,
           close,
           adjusted_close,
           volume
    from {{ ref('historical_prices_aggregated_1m') }}
)
