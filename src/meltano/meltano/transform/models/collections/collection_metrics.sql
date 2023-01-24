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

with collection_daily_latest_chart_point as
         (
             select collection_chart.*,
                    row_number() over (partition by collection_uniq_id order by t.date desc) as idx
             from (
                      select collection_uniq_id, period, date, max(datetime) as datetime
                      from {{ ref('collection_chart') }}
                      where period = '1w'
                      group by collection_uniq_id, period, date
                  ) t
                      join {{ ref('collection_chart') }} using (collection_uniq_id, period, datetime)
     ),
     ticker_metrics as
         (
             select collection_uniq_id,
                    sum(weight * market_capitalization)                        as market_capitalization_sum,
                    greatest(max(ticker_metrics.updated_at),
                             max(collection_ticker_actual_weights.updated_at)) as updated_at
             from {{ ref('collection_ticker_actual_weights') }}
                      join {{ ref('ticker_metrics') }} using (symbol)
             group by collection_uniq_id
     ),
     metrics as
         (
             select collection_uniq_id,
                    value_0d /
                    case
                        when coalesce(value_1w, value_all) > 0
                            then coalesce(value_1w, value_all)
                        end - 1 as value_change_1w,
                    value_0d /
                    case
                        when coalesce(value_1m, value_all) > 0
                            then coalesce(value_1m, value_all)
                        end - 1 as value_change_1m,
                    value_0d /
                    case
                        when coalesce(value_3m, value_all) > 0
                            then coalesce(value_3m, value_all)
                        end - 1 as value_change_3m,
                    value_0d /
                    case
                        when coalesce(value_1y, value_all) > 0
                            then coalesce(value_1y, value_all)
                        end - 1 as value_change_1y,
                    value_0d /
                    case
                        when coalesce(value_5y, value_all) > 0
                            then coalesce(value_5y, value_all)
                        end - 1 as value_change_5y,
                    value_0d /
                    case
                        when value_all > 0
                            then value_all
                        end - 1 as value_change_all,
                    updated_at
             from {{ ref('collection_historical_values_marked') }}
     ),
     ranked_performance as
         (
             select profile_collections.id                      as collection_id,
                    rank() over (order by value_change_1m desc) as rank
             from metrics
                      join {{ ref('profile_collections') }} on profile_collections.uniq_id = collection_uniq_id
             where profile_collections.enabled = '1'
               and profile_collections.personalized = '0'
         ),
     ranked_clicks as
         (
             select distinct on (collection_id) collection_id, rank
             from {{ ref('top_global_collections') }}
                      join {{ ref('collections') }} on collections.id = top_global_collections.collection_id
             where collections.enabled = '1'
               and collections.personalized = '0'
     )
select profile_collections.profile_id,
       profiles.user_id,
       profile_collections.uniq_id::text                                           as collection_uniq_id,
       latest_day.adjusted_close::double precision                                 as actual_price,
       (latest_day.adjusted_close - previous_day.adjusted_close)::double precision as absolute_daily_change,
       (latest_day.adjusted_close /
        case when previous_day.adjusted_close > 0 then previous_day.adjusted_close end - 1
           )::double precision                                                     as relative_daily_change,
       metrics.value_change_1w,
       metrics.value_change_1m,
       metrics.value_change_3m,
       metrics.value_change_1y,
       metrics.value_change_5y,
       metrics.value_change_all,
       previous_day.adjusted_close::double precision                               as previous_day_close_price,
       ticker_metrics.market_capitalization_sum::bigint,
       ranked_performance.rank::int                                                as performance_rank,
       ranked_clicks.rank::int                                                     as clicks_rank,
       greatest(latest_day.updated_at, previous_day.updated_at,
           ticker_metrics.updated_at, metrics.updated_at)                          as updated_at
from {{ ref('profile_collections') }}
         left join {{ source('app', 'profiles') }} on profiles.id = profile_collections.profile_id
         left join ranked_performance on ranked_performance.collection_id = profile_collections.id
         left join ranked_clicks on ranked_clicks.collection_id = profile_collections.id
         left join ticker_metrics
                   on ticker_metrics.collection_uniq_id = profile_collections.uniq_id
         left join metrics
                   on metrics.collection_uniq_id = profile_collections.uniq_id
         left join collection_daily_latest_chart_point latest_day
                   on latest_day.collection_uniq_id = profile_collections.uniq_id and latest_day.idx = 1
         left join collection_daily_latest_chart_point previous_day
                   on previous_day.collection_uniq_id = profile_collections.uniq_id and previous_day.idx = 2
{% if is_incremental() %}
         left join {{ this }} old_data on old_data.collection_uniq_id = profile_collections.uniq_id
{% endif %}

where profile_collections.enabled = '1'

{% if is_incremental() %}
  and (old_data.collection_uniq_id is null
       or previous_day.updated_at >= old_data.updated_at
       or latest_day.updated_at >= old_data.updated_at)
{% endif %}
