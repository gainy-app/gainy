{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}


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
             left join {{ ref('week_trading_sessions') }}
                       on week_trading_sessions.symbol = historical_prices_aggregated_3min.symbol
    where (historical_prices_aggregated_3min.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at - interval '3 minutes'
       or (week_trading_sessions is null and historical_prices_aggregated_3min.datetime > now() - interval '1 day'))
      and (week_trading_sessions is null or (week_trading_sessions.date = historical_prices_aggregated_3min.datetime::date and week_trading_sessions.index = 0))
)

union all

(
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
             left join {{ ref('week_trading_sessions') }}
                       on week_trading_sessions.symbol = historical_prices_aggregated_15min.symbol
    where (historical_prices_aggregated_15min.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at - interval '15 minutes'
       or (week_trading_sessions is null and historical_prices_aggregated_15min.datetime > now() - interval '7 days'))
      and (week_trading_sessions is null or week_trading_sessions.date = historical_prices_aggregated_15min.datetime::date)
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
             left join {{ ref('week_trading_sessions') }}
                       on week_trading_sessions.symbol = historical_prices_aggregated_1d.symbol
                           and week_trading_sessions.index = 0
    where historical_prices_aggregated_1d.datetime >= coalesce(week_trading_sessions.date, now()) - interval '1 month'
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
             left join {{ ref('week_trading_sessions') }}
                       on week_trading_sessions.symbol = historical_prices_aggregated_1d.symbol
                           and week_trading_sessions.index = 0
    where historical_prices_aggregated_1d.datetime >= coalesce(week_trading_sessions.date, now()) - interval '3 month'
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
             left join {{ ref('week_trading_sessions') }}
                       on week_trading_sessions.symbol = historical_prices_aggregated_1d.symbol
                           and week_trading_sessions.index = 0
    where historical_prices_aggregated_1d.datetime >= coalesce(week_trading_sessions.date, now()) - interval '1 year'
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
             left join {{ ref('week_trading_sessions') }}
                       on week_trading_sessions.symbol = historical_prices_aggregated_1w.symbol
                           and week_trading_sessions.index = 0
    where historical_prices_aggregated_1w.datetime >= coalesce(week_trading_sessions.date, now()) - interval '5 year'
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
