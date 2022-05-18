{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}

with grouped_collections as
         (
             select profile_id,
                    user_id,
                    collection_uniq_id,
                    sum(absolute_daily_change * weight)                              as absolute_daily_change,
                    sum(actual_price * weight)                                       as actual_price,
                    sum(actual_price * weight) - sum(absolute_daily_change * weight) as prev_close_price,
                    max(time)                                                        as time,
                    sum(market_capitalization)                                       as market_capitalization_sum
             from {{ ref('collection_tickers_weighted') }}
                      left join {{ source('app', 'profiles') }} on profiles.id = profile_id
                      join {{ ref('ticker_realtime_metrics') }} using (symbol)
                      join {{ ref('ticker_metrics') }} using (symbol)
             group by profile_id, user_id, collection_uniq_id
         )
select profile_id,
       user_id,
       collection_uniq_id,
       actual_price::double precision                                                                  as actual_price,
       absolute_daily_change::double precision                                                         as absolute_daily_change,
       (actual_price / case when prev_close_price > 0 then prev_close_price end - 1)::double precision as relative_daily_change,
       prev_close_price                                                                                as previous_day_close_price,
       time                                                                                            as updated_at,
       market_capitalization_sum
from grouped_collections
