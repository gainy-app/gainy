{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('collection_uniq_id, date'),
      index(this, 'id', true),
    ]
  )
}}


with data0 as
         (
             select profile_id,
                    collection_id,
                    collection_uniq_id,
                    date,
                    price,
                    lag(price) over (partition by collection_uniq_id, symbol order by date)  as prev_price,
                    lag(weight) over (partition by collection_uniq_id, symbol order by date) as prev_weight,
                    updated_at
             from {{ ref('collection_ticker_weights') }}
         ),
     data as materialized
         (
             select profile_id,
                    collection_id,
                    collection_uniq_id,
                    date,
                    prev_weight * (price / prev_price - 1) as relative_gain,
                    updated_at
             from data0
         ),
     daily_collection_gain as
         (
             select profile_id,
                    collection_id,
                    collection_uniq_id,
                    date,
                    sum(relative_gain) + 1 as daily_collection_gain,
                    max(updated_at)        as updated_at
             from data
             group by profile_id,collection_id,collection_uniq_id, date
         ),
     daily_collection_gain_cumulative as
         (
             select profile_id,
                    collection_id,
                    collection_uniq_id,
                    date,
                    exp(sum(ln(daily_collection_gain))
                        over (partition by collection_uniq_id order by date)) as cumulative_daily_relative_gain,
                    updated_at
             from daily_collection_gain
         )
select daily_collection_gain_cumulative.profile_id,
       daily_collection_gain_cumulative.collection_id,
       collection_uniq_id,
       date,
       date_trunc('week', date)::date              as date_week,
       date_trunc('month', date)::date             as date_month,
       coalesce(cumulative_daily_relative_gain, 1) as value,
       collection_uniq_id || '_' || date           as id,
       daily_collection_gain_cumulative.updated_at
from daily_collection_gain_cumulative

{% if is_incremental() %}
         left join {{ this }} old_data using (collection_uniq_id, date)
where old_data is null
   or abs(coalesce(cumulative_daily_relative_gain, 1) - old_data.value) > 1e-6
{% endif %}
