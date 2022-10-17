{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}


select drivewealth_holdings.profile_id,
       drivewealth_holdings.collection_id,
       drivewealth_holdings.symbol,
       drivewealth_holdings.actual_value,
       coalesce(ticker_realtime_metrics.relative_daily_change, 0)       as relative_gain_1d,
       coalesce(ticker_metrics.price_change_1w, 0)                      as relative_gain_1w,
       coalesce(ticker_metrics.price_change_1m, 0)                      as relative_gain_1m,
       coalesce(ticker_metrics.price_change_3m, 0)                      as relative_gain_3m,
       coalesce(ticker_metrics.price_change_1y, 0)                      as relative_gain_1y,
       coalesce(ticker_metrics.price_change_5y, 0)                      as relative_gain_5y,
       coalesce(ticker_metrics.price_change_all, 0)                     as relative_gain_total,
       drivewealth_holdings.actual_value -
       drivewealth_holdings.actual_value /
       (1 + coalesce(ticker_realtime_metrics.relative_daily_change, 0)) as absolute_gain_1d,
       drivewealth_holdings.actual_value -
       drivewealth_holdings.actual_value / (1 + coalesce(ticker_metrics.price_change_1w, 0))
                                                                        as absolute_gain_1w,
       drivewealth_holdings.actual_value -
       drivewealth_holdings.actual_value / (1 + coalesce(ticker_metrics.price_change_1m, 0))
                                                                        as absolute_gain_1m,
       drivewealth_holdings.actual_value -
       drivewealth_holdings.actual_value / (1 + coalesce(ticker_metrics.price_change_3m, 0))
                                                                        as absolute_gain_3m,
       drivewealth_holdings.actual_value -
       drivewealth_holdings.actual_value / (1 + coalesce(ticker_metrics.price_change_1y, 0))
                                                                        as absolute_gain_1y,
       drivewealth_holdings.actual_value -
       drivewealth_holdings.actual_value / (1 + coalesce(ticker_metrics.price_change_5y, 0))
                                                                        as absolute_gain_5y,
       drivewealth_holdings.actual_value -
       drivewealth_holdings.actual_value / (1 + coalesce(ticker_metrics.price_change_all, 0))
                                                                        as absolute_gain_total,
       null                                                             as ltt_quantity_total,
       now()::timestamp                                                 as updated_at
from {{ ref('drivewealth_holdings') }}
         left join {{ ref('ticker_realtime_metrics') }} using (symbol)
         left join {{ ref('ticker_metrics') }} using (symbol)
