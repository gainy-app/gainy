{{
  config(
    materialized = "view",
  )
}}

    
select drivewealth_portfolio_historical_prices_aggregated.profile_id,
       drivewealth_portfolio_historical_prices_aggregated.transaction_uniq_id,
       week_trading_sessions_static.date,
       drivewealth_portfolio_historical_prices_aggregated.datetime,
       drivewealth_portfolio_historical_prices_aggregated.datetime + interval '3 minutes' as close_datetime,
       '1d'::varchar as period,
       drivewealth_portfolio_historical_prices_aggregated.open,
       drivewealth_portfolio_historical_prices_aggregated.high,
       drivewealth_portfolio_historical_prices_aggregated.low,
       drivewealth_portfolio_historical_prices_aggregated.close,
       drivewealth_portfolio_historical_prices_aggregated.adjusted_close,
       week_trading_sessions_static.index                                                 as week_trading_session_index,
       null::timestamp                                                                    as latest_trading_time
from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
         join {{ ref('week_trading_sessions_static') }} using (symbol)
where drivewealth_portfolio_historical_prices_aggregated.period = '3min'
  and drivewealth_portfolio_historical_prices_aggregated.datetime between week_trading_sessions_static.open_at and week_trading_sessions_static.close_at - interval '1 microsecond'

union all

select drivewealth_portfolio_historical_prices_aggregated.profile_id,
       drivewealth_portfolio_historical_prices_aggregated.transaction_uniq_id,
       week_trading_sessions_static.date,
       drivewealth_portfolio_historical_prices_aggregated.datetime,
       drivewealth_portfolio_historical_prices_aggregated.datetime + interval '15 minutes' as close_datetime,
       '1w'::varchar as period,
       drivewealth_portfolio_historical_prices_aggregated.open,
       drivewealth_portfolio_historical_prices_aggregated.high,
       drivewealth_portfolio_historical_prices_aggregated.low,
       drivewealth_portfolio_historical_prices_aggregated.close,
       drivewealth_portfolio_historical_prices_aggregated.adjusted_close,
       week_trading_sessions_static.index                                                  as week_trading_session_index,
       null::timestamp                                                                     as latest_trading_time
from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
         join {{ ref('week_trading_sessions_static') }} using (symbol)
where drivewealth_portfolio_historical_prices_aggregated.period = '15min'
  and drivewealth_portfolio_historical_prices_aggregated.datetime between week_trading_sessions_static.open_at and week_trading_sessions_static.close_at - interval '1 microsecond'

union all

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
       null::int                                                                      as week_trading_session_index,
       ticker_realtime_metrics.time                                                   as latest_trading_time
from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
         left join {{ ref('ticker_realtime_metrics') }} using (symbol)
where drivewealth_portfolio_historical_prices_aggregated.period = '1d'
  and drivewealth_portfolio_historical_prices_aggregated.datetime >= now() - interval '1 month + 1 week'

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
       null::int                                                                      as week_trading_session_index,
       ticker_realtime_metrics.time                                                   as latest_trading_time
from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
         left join {{ ref('ticker_realtime_metrics') }} using (symbol)
where drivewealth_portfolio_historical_prices_aggregated.period = '1d'
  and drivewealth_portfolio_historical_prices_aggregated.datetime >= now() - interval '3 month + 1 week'

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
       null::int                                                                      as week_trading_session_index,
       ticker_realtime_metrics.time                                                   as latest_trading_time
from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
         left join {{ ref('ticker_realtime_metrics') }} using (symbol)
where drivewealth_portfolio_historical_prices_aggregated.period = '1d'

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
       null::int                                                                       as week_trading_session_index,
       ticker_realtime_metrics.time                                                    as latest_trading_time
from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
         left join {{ ref('ticker_realtime_metrics') }} using (symbol)
where drivewealth_portfolio_historical_prices_aggregated.period = '1w'

union all

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
       null::int                     as week_trading_session_index,
       ticker_realtime_metrics.time  as latest_trading_time
from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
         left join {{ ref('ticker_realtime_metrics') }} using (symbol)
where drivewealth_portfolio_historical_prices_aggregated.period = '1m'
