{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}

    
(
    select drivewealth_portfolio_historical_prices_aggregated.profile_id,
           drivewealth_portfolio_historical_prices_aggregated.transaction_uniq_id,
           week_trading_sessions.date,
           drivewealth_portfolio_historical_prices_aggregated.datetime,
           drivewealth_portfolio_historical_prices_aggregated.datetime + interval '3 minutes' as close_datetime,
           '1d'::varchar as period,
           drivewealth_portfolio_historical_prices_aggregated.open,
           drivewealth_portfolio_historical_prices_aggregated.high,
           drivewealth_portfolio_historical_prices_aggregated.low,
           drivewealth_portfolio_historical_prices_aggregated.close,
           drivewealth_portfolio_historical_prices_aggregated.adjusted_close,
           drivewealth_portfolio_historical_prices_aggregated.updated_at
    from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
             join {{ ref('week_trading_sessions') }} using (symbol)
    where drivewealth_portfolio_historical_prices_aggregated.period = '3min'
      and week_trading_sessions.index = 0
      and drivewealth_portfolio_historical_prices_aggregated.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at - interval '1 microsecond'
)

union all

(
    select drivewealth_portfolio_historical_prices_aggregated.profile_id,
           drivewealth_portfolio_historical_prices_aggregated.transaction_uniq_id,
           week_trading_sessions.date,
           drivewealth_portfolio_historical_prices_aggregated.datetime,
           drivewealth_portfolio_historical_prices_aggregated.datetime + interval '15 minutes' as close_datetime,
           '1w'::varchar as period,
           drivewealth_portfolio_historical_prices_aggregated.open,
           drivewealth_portfolio_historical_prices_aggregated.high,
           drivewealth_portfolio_historical_prices_aggregated.low,
           drivewealth_portfolio_historical_prices_aggregated.close,
           drivewealth_portfolio_historical_prices_aggregated.adjusted_close,
           drivewealth_portfolio_historical_prices_aggregated.updated_at
    from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
             join {{ ref('week_trading_sessions') }} using (symbol)
    where drivewealth_portfolio_historical_prices_aggregated.period = '15min'
      and drivewealth_portfolio_historical_prices_aggregated.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at - interval '1 microsecond'
)

union all

(
    with latest_open_trading_session as
             (
                 select symbol, max(date) as date
                 from week_trading_sessions
                 where index = 0
                 group by symbol
             )
    select drivewealth_portfolio_historical_prices_aggregated.profile_id,
           drivewealth_portfolio_historical_prices_aggregated.transaction_uniq_id,
           drivewealth_portfolio_historical_prices_aggregated.datetime::date as date,
           drivewealth_portfolio_historical_prices_aggregated.datetime,
           drivewealth_portfolio_historical_prices_aggregated.datetime + interval '1 day' as close_datetime,
           '1m'::varchar as period,
           drivewealth_portfolio_historical_prices_aggregated.open,
           drivewealth_portfolio_historical_prices_aggregated.high,
           drivewealth_portfolio_historical_prices_aggregated.low,
           drivewealth_portfolio_historical_prices_aggregated.close,
           drivewealth_portfolio_historical_prices_aggregated.adjusted_close,
           drivewealth_portfolio_historical_prices_aggregated.updated_at
    from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
             left join latest_open_trading_session using (symbol)
    where drivewealth_portfolio_historical_prices_aggregated.period = '1d'
      and drivewealth_portfolio_historical_prices_aggregated.datetime >= coalesce(latest_open_trading_session.date, now()) - interval '1 month'

    union all

    select drivewealth_portfolio_historical_prices_aggregated.profile_id,
           drivewealth_portfolio_historical_prices_aggregated.transaction_uniq_id,
           drivewealth_portfolio_historical_prices_aggregated.datetime::date as date,
           drivewealth_portfolio_historical_prices_aggregated.datetime,
           drivewealth_portfolio_historical_prices_aggregated.datetime + interval '1 day' as close_datetime,
           '3m'::varchar as period,
           drivewealth_portfolio_historical_prices_aggregated.open,
           drivewealth_portfolio_historical_prices_aggregated.high,
           drivewealth_portfolio_historical_prices_aggregated.low,
           drivewealth_portfolio_historical_prices_aggregated.close,
           drivewealth_portfolio_historical_prices_aggregated.adjusted_close,
           drivewealth_portfolio_historical_prices_aggregated.updated_at
    from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
             left join latest_open_trading_session using (symbol)
    where drivewealth_portfolio_historical_prices_aggregated.period = '1d'
      and drivewealth_portfolio_historical_prices_aggregated.datetime >= coalesce(latest_open_trading_session.date, now()) - interval '3 month'

    union all

    select drivewealth_portfolio_historical_prices_aggregated.profile_id,
           drivewealth_portfolio_historical_prices_aggregated.transaction_uniq_id,
           drivewealth_portfolio_historical_prices_aggregated.datetime::date as date,
           drivewealth_portfolio_historical_prices_aggregated.datetime,
           drivewealth_portfolio_historical_prices_aggregated.datetime + interval '1 day' as close_datetime,
           '1y'::varchar as period,
           drivewealth_portfolio_historical_prices_aggregated.open,
           drivewealth_portfolio_historical_prices_aggregated.high,
           drivewealth_portfolio_historical_prices_aggregated.low,
           drivewealth_portfolio_historical_prices_aggregated.close,
           drivewealth_portfolio_historical_prices_aggregated.adjusted_close,
           drivewealth_portfolio_historical_prices_aggregated.updated_at
    from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
             left join latest_open_trading_session using (symbol)
    where drivewealth_portfolio_historical_prices_aggregated.period = '1d'
      and drivewealth_portfolio_historical_prices_aggregated.datetime >= coalesce(latest_open_trading_session.date, now()) - interval '1 year'

    union all

    select drivewealth_portfolio_historical_prices_aggregated.profile_id,
           drivewealth_portfolio_historical_prices_aggregated.transaction_uniq_id,
           drivewealth_portfolio_historical_prices_aggregated.datetime::date as date,
           drivewealth_portfolio_historical_prices_aggregated.datetime,
           drivewealth_portfolio_historical_prices_aggregated.datetime + interval '1 week' as close_datetime,
           '5y'::varchar as period,
           drivewealth_portfolio_historical_prices_aggregated.open,
           drivewealth_portfolio_historical_prices_aggregated.high,
           drivewealth_portfolio_historical_prices_aggregated.low,
           drivewealth_portfolio_historical_prices_aggregated.close,
           drivewealth_portfolio_historical_prices_aggregated.adjusted_close,
           drivewealth_portfolio_historical_prices_aggregated.updated_at
    from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
             left join latest_open_trading_session using (symbol)
    where drivewealth_portfolio_historical_prices_aggregated.period = '1w'
      and drivewealth_portfolio_historical_prices_aggregated.datetime >= coalesce(latest_open_trading_session.date, now()) - interval '5 year'
)

union all

(
    select profile_id,
           transaction_uniq_id,
           datetime::date                as date,
           datetime,
           datetime + interval '1 month' as close_datetime,
           'all'::varchar                as period,
           open,
           high,
           low,
           close,
           adjusted_close,
           updated_at
    from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
    where drivewealth_portfolio_historical_prices_aggregated.period = '1m'
)
