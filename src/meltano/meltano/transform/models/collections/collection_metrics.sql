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
     metrics as
         (
             select collection_uniq_id,
                    sum(weight * market_capitalization)                        as market_capitalization_sum,
                    greatest(max(ticker_metrics.updated_at),
                             max(collection_ticker_actual_weights.updated_at)) as updated_at
             from {{ ref('collection_ticker_actual_weights') }}
                      join {{ ref('ticker_metrics') }} using (symbol)
             group by collection_uniq_id
     )
select profile_collections.profile_id,
       profiles.user_id,
       profile_collections.uniq_id::text                                           as collection_uniq_id,
       latest_day.adjusted_close::double precision                                 as actual_price,
       (latest_day.adjusted_close - previous_day.adjusted_close)::double precision as absolute_daily_change,
       (latest_day.adjusted_close /
        case when previous_day.adjusted_close > 0 then previous_day.adjusted_close end - 1
           )::double precision                                                     as relative_daily_change,
       previous_day.adjusted_close::double precision                               as previous_day_close_price,
       market_capitalization_sum::bigint,
       greatest(latest_day.updated_at, previous_day.updated_at)                    as updated_at
from {{ ref('profile_collections') }}
         left join {{ source('app', 'profiles') }} on profiles.id = profile_collections.profile_id
         left join metrics
                   on metrics.collection_uniq_id = profile_collections.uniq_id
         left join collection_daily_latest_chart_point latest_day
                   on latest_day.collection_uniq_id = profile_collections.uniq_id and latest_day.idx = 1
         left join collection_daily_latest_chart_point previous_day
                   on previous_day.collection_uniq_id = profile_collections.uniq_id and previous_day.idx = 2
{% if is_incremental() %}
         left join {{ this }} old_data using (collection_uniq_id)
{% endif %}

where profile_collections.enabled = '1'

{% if is_incremental() %}
  and (old_data is null
       or previous_day.updated_at >= old_data.updated_at
       or latest_day.updated_at >= old_data.updated_at)
{% endif %}
