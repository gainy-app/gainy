{{
  config(
    materialized = "view",
  )
}}

with all_ticker_collections as
         (
             select *,
                    (coalesce(profile_id, 0) || '_' || collection_id)::varchar as uniq_id
             from (
                      select profile_id, collection_id, symbol
                      from {{ source('app', 'personalized_ticker_collections') }}
                      union all
                      select null, collection_id, symbol
                      from {{ ref('ticker_collections') }}
                  ) t
         ),
     weighted_collections as
         (
             select all_ticker_collections.profile_id,
                    all_ticker_collections.uniq_id                           as collection_uniq_id,
                    ticker_realtime_metrics.actual_price::numeric,
                    (ticker_realtime_metrics.actual_price -
                     ticker_realtime_metrics.absolute_daily_change)::numeric as day_open_price,
                    ticker_metrics.market_capitalization::numeric /
                    sum(ticker_metrics.market_capitalization::numeric)
                    over (partition by all_ticker_collections.uniq_id)       as weight,
                    ticker_realtime_metrics.time                             as datetime
             from all_ticker_collections
                      join {{ ref('ticker_realtime_metrics') }}
                           on ticker_realtime_metrics.symbol = all_ticker_collections.symbol
                      join {{ ref('ticker_metrics') }} on ticker_metrics.symbol = all_ticker_collections.symbol
         ),
     grouped_collections as
         (
             select profile_id,
                    collection_uniq_id,
                    sum(day_open_price * weight) as open_value,
                    sum(actual_price * weight)   as close_value,
                    max(datetime)                as datetime
             from weighted_collections
             group by profile_id, collection_uniq_id
         )
select profile_id,
       collection_uniq_id,
       (close_value - open_value)::double precision                                       as absolute_daily_change,
       (close_value / case when open_value > 0 then open_value end - 1)::double precision as relative_daily_change,
       datetime                                                                           as updated_at
from grouped_collections
