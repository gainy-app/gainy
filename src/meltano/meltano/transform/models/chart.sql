{{
  config(
    materialized = "view",
  )
}}


(
    select historical_prices_aggregated_3min.symbol,
           week_trading_sessions.date,
           historical_prices_aggregated_3min.datetime,
           historical_prices_aggregated_3min.datetime + interval '3 minutes' as close_datetime,
           '1d'::varchar as period,
           historical_prices_aggregated_3min.open,
           historical_prices_aggregated_3min.high,
           historical_prices_aggregated_3min.low,
           historical_prices_aggregated_3min.close,
           historical_prices_aggregated_3min.adjusted_close,
           historical_prices_aggregated_3min.volume,
           historical_prices_aggregated_3min.updated_at
    from {{ ref('historical_prices_aggregated_3min') }}
             join {{ ref('week_trading_sessions') }} using (symbol)
    where week_trading_sessions.index = 0
      and historical_prices_aggregated_3min.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at - interval '1 microsecond'
)

union all

(
    select historical_prices_aggregated_15min.symbol,
           week_trading_sessions.date,
           historical_prices_aggregated_15min.datetime,
           historical_prices_aggregated_15min.datetime + interval '15 minutes' as close_datetime,
           '1w'::varchar as period,
           historical_prices_aggregated_15min.open,
           historical_prices_aggregated_15min.high,
           historical_prices_aggregated_15min.low,
           historical_prices_aggregated_15min.close,
           historical_prices_aggregated_15min.adjusted_close,
           historical_prices_aggregated_15min.volume,
           historical_prices_aggregated_15min.updated_at
    from {{ ref('historical_prices_aggregated_15min') }}
             join {{ ref('week_trading_sessions') }} using (symbol)
    where week_trading_sessions.date >= now()::date - interval '1 week'
      and historical_prices_aggregated_15min.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at - interval '1 microsecond'
)

union all

(
    select historical_prices_aggregated_1d.symbol,
           historical_prices_aggregated_1d.datetime::date as date,
           historical_prices_aggregated_1d.datetime,
           historical_prices_aggregated_1d.datetime + interval '1 day' as close_datetime,
           '1m'::varchar as period,
           historical_prices_aggregated_1d.open,
           historical_prices_aggregated_1d.high,
           historical_prices_aggregated_1d.low,
           historical_prices_aggregated_1d.close,
           historical_prices_aggregated_1d.adjusted_close,
           historical_prices_aggregated_1d.volume,
           historical_prices_aggregated_1d.updated_at
    from {{ ref('historical_prices_aggregated_1d') }}
    where historical_prices_aggregated_1d.datetime >= now()::date - interval '1 month'
)

union all

(
    select historical_prices_aggregated_1d.symbol,
           historical_prices_aggregated_1d.datetime::date as date,
           historical_prices_aggregated_1d.datetime,
           historical_prices_aggregated_1d.datetime + interval '1 day' as close_datetime,
           '3m'::varchar as period,
           historical_prices_aggregated_1d.open,
           historical_prices_aggregated_1d.high,
           historical_prices_aggregated_1d.low,
           historical_prices_aggregated_1d.close,
           historical_prices_aggregated_1d.adjusted_close,
           historical_prices_aggregated_1d.volume,
           historical_prices_aggregated_1d.updated_at
    from {{ ref('historical_prices_aggregated_1d') }}
    where historical_prices_aggregated_1d.datetime >= now()::date - interval '3 month'
)

union all

(
    select historical_prices_aggregated_1d.symbol,
           historical_prices_aggregated_1d.datetime::date as date,
           historical_prices_aggregated_1d.datetime,
           historical_prices_aggregated_1d.datetime + interval '1 day' as close_datetime,
           '1y'::varchar as period,
           historical_prices_aggregated_1d.open,
           historical_prices_aggregated_1d.high,
           historical_prices_aggregated_1d.low,
           historical_prices_aggregated_1d.close,
           historical_prices_aggregated_1d.adjusted_close,
           historical_prices_aggregated_1d.volume,
           historical_prices_aggregated_1d.updated_at
    from {{ ref('historical_prices_aggregated_1d') }}
    where historical_prices_aggregated_1d.datetime >= now()::date - interval '1 year'
)

union all

(
    select historical_prices_aggregated_1w.symbol,
           historical_prices_aggregated_1w.datetime::date as date,
           historical_prices_aggregated_1w.datetime,
           historical_prices_aggregated_1w.datetime + interval '1 week' as close_datetime,
           '5y'::varchar as period,
           historical_prices_aggregated_1w.open,
           historical_prices_aggregated_1w.high,
           historical_prices_aggregated_1w.low,
           historical_prices_aggregated_1w.close,
           historical_prices_aggregated_1w.adjusted_close,
           historical_prices_aggregated_1w.volume,
           historical_prices_aggregated_1w.updated_at
    from {{ ref('historical_prices_aggregated_1w') }}
    where historical_prices_aggregated_1w.datetime >= now()::date - interval '5 year'
)

union all

(
    select symbol,
           datetime::date                as date,
           datetime,
           datetime + interval '1 month' as close_datetime,
           'all'::varchar                as period,
           open,
           high,
           low,
           close,
           adjusted_close,
           volume,
           updated_at
    from {{ ref('historical_prices_aggregated_1m') }}
)
