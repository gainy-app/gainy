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

with latest_day as
         (
             select distinct on (collection_uniq_id) collection_uniq_id, adjusted_close, date, updated_at
             from {{ ref('collection_chart') }}
             where period = '1d'
             order by collection_uniq_id, datetime desc
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
             select collection_historical_values_marked.collection_uniq_id,
                    latest_day.adjusted_close::double precision as actual_price,
                    (latest_day.adjusted_close - case
                                                     when latest_day.date = collection_historical_values_marked.date_1d
                                                         then value_2d
                                                     else value_1d
                        end)::double precision                  as absolute_daily_change,
                    (latest_day.adjusted_close /
                     case
                         when latest_day.date = collection_historical_values_marked.date_1d
                             then case when value_2d > 0 then value_2d end
                         else case when value_1d > 0 then value_1d end
                         end - 1
                        )::double precision                     as relative_daily_change,
                    latest_day.adjusted_close /
                    case
                        when coalesce(value_1w, value_all) > 0
                            then coalesce(value_1w, value_all)
                        end - 1                                 as value_change_1w,
                    latest_day.adjusted_close /
                    case
                        when coalesce(value_1m, value_all) > 0
                            then coalesce(value_1m, value_all)
                        end - 1                                 as value_change_1m,
                    latest_day.adjusted_close /
                    case
                        when coalesce(value_3m, value_all) > 0
                            then coalesce(value_3m, value_all)
                        end - 1                                 as value_change_3m,
                    latest_day.adjusted_close /
                    case
                        when coalesce(value_1y, value_all) > 0
                            then coalesce(value_1y, value_all)
                        end - 1                                 as value_change_1y,
                    latest_day.adjusted_close /
                    case
                        when coalesce(value_5y, value_all) > 0
                            then coalesce(value_5y, value_all)
                        end - 1                                 as value_change_5y,
                    latest_day.adjusted_close /
                    case
                        when value_all > 0
                            then value_all
                        end - 1                                 as value_change_all,
                    case
                        when latest_day.date = collection_historical_values_marked.date_1d
                            then value_2d
                        else value_1d
                        end::double precision                   as previous_day_close_price,
                    value_1w                                    as prev_value_1w,
                    value_1m                                    as prev_value_1m,
                    value_3m                                    as prev_value_3m,
                    value_1y                                    as prev_value_1y,
                    value_5y                                    as prev_value_5y,
                    value_all                                   as prev_value_total,
                    latest_day.updated_at
             from {{ ref('collection_historical_values_marked') }}
                      left join latest_day using (collection_uniq_id)
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
     ),
     volatility_90 as
         (
             select collection_uniq_id,
                    stddev(relative_daily_gain) * pow(252, 0.5) as volatility_90
             from {{ ref('collection_historical_values') }}
             where date >= now() - interval '90 days'
             group by collection_uniq_id
     ),
     beta as
         (
             with data as
                      (
                          with market_gains as
                                   (
                                       select date_month,
                                              exp(sum(ln(coalesce(relative_daily_gain, 0) + 1 + 1e-30))) - 1 as relative_gain
                                       from {{ ref('historical_prices') }}
                                       where historical_prices.symbol = 'SPY'
                                       group by date_month
                                   ),
                               collection_gains as
                                   (
                                       select collection_uniq_id,
                                              date_month,
                                              exp(sum(ln(coalesce(relative_daily_gain, 0) + 1 + 1e-30))) - 1 as relative_gain
                                       from {{ ref('collection_historical_values') }}
                                       group by collection_uniq_id, date_month
                               )
                          select collection_uniq_id,
                                 collection_gains.relative_gain as collection_relative_gain,
                                 market_gains.relative_gain     as spy_relative_gain
                          from collection_gains
                                   join market_gains using (date_month)
                      ),
                  correlation as
                      (
                          select collection_uniq_id,
                                 corr(collection_relative_gain, spy_relative_gain) as correlation
                          from data
                          group by collection_uniq_id
                  ),
                  variances as
                      (
                          select collection_uniq_id,
                                 stddev(collection_relative_gain) as collection_covariance,
                                 stddev(spy_relative_gain)        as market_variance
                          from data
                          group by collection_uniq_id
                  )
             select collection_uniq_id,
                    correlation * collection_covariance / market_variance as beta
             from correlation
                      join variances using (collection_uniq_id)
     )
select profile_collections.profile_id,
       profiles.user_id,
       profile_collections.id             as collection_id,
       profile_collections.uniq_id::text  as collection_uniq_id,
       metrics.actual_price,
       metrics.absolute_daily_change,
       metrics.relative_daily_change,
       metrics.value_change_1w,
       metrics.value_change_1m,
       metrics.value_change_3m,
       metrics.value_change_1y,
       metrics.value_change_5y,
       metrics.value_change_all,
       metrics.previous_day_close_price,
       metrics.prev_value_1w::double precision,
       metrics.prev_value_1m::double precision,
       metrics.prev_value_3m::double precision,
       metrics.prev_value_1y::double precision,
       metrics.prev_value_5y::double precision,
       metrics.prev_value_total::double precision,
       ticker_metrics.market_capitalization_sum::bigint,
       ranked_performance.rank::int       as performance_rank,
       ranked_clicks.rank::int            as clicks_rank,
       volatility_90.volatility_90,
       beta,
       greatest(metrics.updated_at,
           ticker_metrics.updated_at,
           metrics.updated_at)            as updated_at
from {{ ref('profile_collections') }}
         left join {{ source('app', 'profiles') }} on profiles.id = profile_collections.profile_id
         left join ranked_performance on ranked_performance.collection_id = profile_collections.id
         left join ranked_clicks on ranked_clicks.collection_id = profile_collections.id
         left join ticker_metrics
                   on ticker_metrics.collection_uniq_id = profile_collections.uniq_id
         left join metrics
                   on metrics.collection_uniq_id = profile_collections.uniq_id
         left join volatility_90 on volatility_90.collection_uniq_id = profile_collections.uniq_id
         left join beta on beta.collection_uniq_id = profile_collections.uniq_id
{% if is_incremental() %}
         left join {{ this }} old_data on old_data.collection_uniq_id = profile_collections.uniq_id
{% endif %}

where profile_collections.enabled = '1'

{% if is_incremental() %}
  and (old_data.collection_uniq_id is null
       or metrics.updated_at >= old_data.updated_at)
{% endif %}
