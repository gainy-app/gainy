{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}


select portfolio_expanded_transactions.profile_id,
       portfolio_expanded_transactions.transaction_uniq_id,
       hpa.datetime::timestamp,
       '1d'                                                                               as period,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.open)           as open,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.high)           as high,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.low)            as low,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.close)          as close,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.adjusted_close) as adjusted_close,
       week_trading_sessions_static.index                                                 as week_trading_session_index,
       null::timestamp                                                                    as latest_trading_time
from {{ ref('portfolio_expanded_transactions') }}
         join {{ ref('week_trading_sessions_static') }} using (symbol)
         join {{ ref('historical_prices_aggregated_3min') }} hpa
              on hpa.symbol = portfolio_expanded_transactions.symbol
                  and (hpa.datetime > portfolio_expanded_transactions.datetime - interval '3 minutes' or
                       portfolio_expanded_transactions.datetime is null)
where hpa.datetime between week_trading_sessions_static.open_at and week_trading_sessions_static.close_at - interval '1 microsecond'
  and portfolio_expanded_transactions.security_type != 'ttf'

union all

select portfolio_expanded_transactions.profile_id,
       portfolio_expanded_transactions.transaction_uniq_id,
       hpa.datetime::timestamp,
       '1w'                                                                               as period,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.open)           as open,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.high)           as high,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.low)            as low,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.close)          as close,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.adjusted_close) as adjusted_close,
       week_trading_sessions_static.index                                                 as week_trading_session_index,
       null::timestamp                                                                    as latest_trading_time
from {{ ref('portfolio_expanded_transactions') }}
         join {{ ref('week_trading_sessions_static') }} using (symbol)
         join {{ ref('historical_prices_aggregated_15min') }} hpa
              on hpa.symbol = portfolio_expanded_transactions.symbol
                  and (hpa.datetime > portfolio_expanded_transactions.datetime - interval '15 minutes' or
                       portfolio_expanded_transactions.datetime is null)
where hpa.datetime between week_trading_sessions_static.open_at and week_trading_sessions_static.close_at - interval '1 microsecond'
  and portfolio_expanded_transactions.security_type != 'ttf'

union all

select portfolio_expanded_transactions.profile_id,
       portfolio_expanded_transactions.transaction_uniq_id,
       hpa.datetime::timestamp,
       '1m'                                                                               as period,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.open)           as open,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.high)           as high,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.low)            as low,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.close)          as close,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.adjusted_close) as adjusted_close,
       null::int                                                                          as week_trading_session_index,
       ticker_realtime_metrics.time                                                       as latest_trading_time
from {{ ref('portfolio_expanded_transactions') }}
         left join {{ ref('ticker_realtime_metrics') }} using (symbol)
         join {{ ref('historical_prices_aggregated_1d') }} hpa
              on hpa.symbol = portfolio_expanded_transactions.symbol
                  and (hpa.datetime > portfolio_expanded_transactions.datetime - interval '1 day' or
                       portfolio_expanded_transactions.datetime is null)
where hpa.datetime >= now() - interval '1 month + 1 week'
  and portfolio_expanded_transactions.security_type != 'ttf'

union all

select portfolio_expanded_transactions.profile_id,
       portfolio_expanded_transactions.transaction_uniq_id,
       hpa.datetime::timestamp,
       '3m'                                                                               as period,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.open)           as open,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.high)           as high,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.low)            as low,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.close)          as close,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.adjusted_close) as adjusted_close,
       null::int                                                                          as week_trading_session_index,
       ticker_realtime_metrics.time                                                       as latest_trading_time
from {{ ref('portfolio_expanded_transactions') }}
         left join {{ ref('ticker_realtime_metrics') }} using (symbol)
         join {{ ref('historical_prices_aggregated_1d') }} hpa
              on hpa.symbol = portfolio_expanded_transactions.symbol
                  and (hpa.datetime > portfolio_expanded_transactions.datetime - interval '1 day' or
                       portfolio_expanded_transactions.datetime is null)
where hpa.datetime >= now() - interval '3 month + 1 week'
  and portfolio_expanded_transactions.security_type != 'ttf'

union all

select portfolio_expanded_transactions.profile_id,
       portfolio_expanded_transactions.transaction_uniq_id,
       hpa.datetime::timestamp,
       '1y'                                                                               as period,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.open)           as open,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.high)           as high,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.low)            as low,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.close)          as close,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.adjusted_close) as adjusted_close,
       null::int                                                                          as week_trading_session_index,
       ticker_realtime_metrics.time                                                       as latest_trading_time
from {{ ref('portfolio_expanded_transactions') }}
         left join {{ ref('ticker_realtime_metrics') }} using (symbol)
         join {{ ref('historical_prices_aggregated_1d') }} hpa
              on hpa.symbol = portfolio_expanded_transactions.symbol
                  and (hpa.datetime > portfolio_expanded_transactions.datetime - interval '1 day' or
                       portfolio_expanded_transactions.datetime is null)
where portfolio_expanded_transactions.security_type != 'ttf'

union all

select portfolio_expanded_transactions.profile_id,
       portfolio_expanded_transactions.transaction_uniq_id,
       hpa.datetime::timestamp,
       '5y'                                                                               as period,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.open)           as open,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.high)           as high,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.low)            as low,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.close)          as close,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.adjusted_close) as adjusted_close,
       null::int                                                                          as week_trading_session_index,
       ticker_realtime_metrics.time                                                       as latest_trading_time
from {{ ref('portfolio_expanded_transactions') }}
         left join {{ ref('ticker_realtime_metrics') }} using (symbol)
         join {{ ref('historical_prices_aggregated_1w') }} hpa
              on hpa.symbol = portfolio_expanded_transactions.symbol
                  and (hpa.datetime > portfolio_expanded_transactions.datetime - interval '1 week' or
                       portfolio_expanded_transactions.datetime is null)
where portfolio_expanded_transactions.security_type != 'ttf'

union all

select portfolio_expanded_transactions.profile_id,
       portfolio_expanded_transactions.transaction_uniq_id,
       hpa.datetime::timestamp,
       'all'                                                                              as period,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.open)           as open,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.high)           as high,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.low)            as low,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.close)          as close,
       (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.adjusted_close) as adjusted_close,
       null::int                                                                          as week_trading_session_index,
       ticker_realtime_metrics.time                                                       as latest_trading_time
from {{ ref('portfolio_expanded_transactions') }}
         left join {{ ref('ticker_realtime_metrics') }} using (symbol)
         join {{ ref('historical_prices_aggregated_1m') }} hpa
              on hpa.symbol = portfolio_expanded_transactions.symbol
                  and (hpa.datetime > portfolio_expanded_transactions.datetime - interval '1 month' or
                       portfolio_expanded_transactions.datetime is null)
where portfolio_expanded_transactions.security_type != 'ttf'

union all

select profile_id,
       transaction_uniq_id,
       datetime::timestamp,
       period::varchar,
       open,
       high,
       low,
       close,
       adjusted_close,
       week_trading_session_index,
       latest_trading_time
from {{ ref('drivewealth_portfolio_chart') }}