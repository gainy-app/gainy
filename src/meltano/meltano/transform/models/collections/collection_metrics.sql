{{
  config(
    materialized = "incremental",
    unique_key = "collection_uniq_id",
    tags = ["realtime"],
    post_hook=[
      pk('collection_uniq_id'),
    ]
  )
}}

with grouped_collections as
         (
             select profile_collections.profile_id,
                    user_id,
                    collection_uniq_id,
                    sum(absolute_daily_change * weight)                              as absolute_daily_change,
                    sum(actual_price * weight)                                       as actual_price,
                    sum(actual_price * weight) - sum(absolute_daily_change * weight) as prev_close_price,
                    max(time)                                                        as time,
                    sum(market_capitalization)                                       as market_capitalization_sum,
                    -- todo add collection_tickers_weighted.updated_at
                    greatest(max(ticker_realtime_metrics.time),
                        max(ticker_metrics.updated_at),
                        max(collection_ticker_actual_weights.updated_at))            as updated_at
             from {{ ref('collection_ticker_actual_weights') }}
                      join {{ ref('profile_collections') }} on profile_collections.uniq_id = collection_uniq_id
                      left join {{ source('app', 'profiles') }} on profiles.id = profile_collections.profile_id
                      join {{ ref('ticker_realtime_metrics') }} using (symbol)
                      join {{ ref('ticker_metrics') }} using (symbol)
             where profile_collections.enabled = '1'
             group by profile_collections.profile_id, user_id, collection_uniq_id
         )
select grouped_collections.profile_id,
       grouped_collections.user_id,
       grouped_collections.collection_uniq_id,
       grouped_collections.actual_price::double precision          as actual_price,
       grouped_collections.absolute_daily_change::double precision as absolute_daily_change,
       (grouped_collections.actual_price /
        case when grouped_collections.prev_close_price > 0 then grouped_collections.prev_close_price end - 1
           )::double precision                                     as relative_daily_change,
       grouped_collections.prev_close_price                        as previous_day_close_price,
       grouped_collections.updated_at,
       grouped_collections.market_capitalization_sum::bigint
from grouped_collections

{% if is_incremental() %}
         left join {{ this }} old_data using (collection_uniq_id)
where grouped_collections.updated_at >= old_data.updated_at or old_data is null
{% endif %}
