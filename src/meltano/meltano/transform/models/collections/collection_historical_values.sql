{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('collection_uniq_id, date'),
      index('id', true),
    ]
  )
}}


with data as materialized
         (
             select profile_id,
                    collection_id,
                    collection_uniq_id,
                    collection_ticker_weights.date,
                    weight * (historical_prices.adjusted_close::numeric / price - 1) as relative_gain,
                    coalesce(historical_dividends.value, 0)::numeric                 as dividends_value,
                    greatest(collection_ticker_weights.updated_at,
                             historical_dividends.updated_at,
                             historical_prices.updated_at)                           as updated_at
             from {{ ref('collection_ticker_weights') }}
                      join {{ ref('historical_prices') }} using (symbol, date)
                      left join {{ ref('historical_dividends') }} using (symbol, date)
         ),
     daily_collection_gain as
         (
             select profile_id,
                    collection_id,
                    collection_uniq_id,
                    date,
                    sum(relative_gain) + 1 as daily_collection_gain,
                    sum(dividends_value)   as dividends_value,
                    max(updated_at)        as updated_at
             from data
             group by profile_id, collection_id, collection_uniq_id, date
     ),
     collection_symbol_stats as
         (
             select collection_uniq_id, min(date) as min_date, max(date) as max_date
             from {{ ref('collection_ticker_weights') }}
             group by collection_uniq_id
     ),
     schedule as
         (
             select *,
                    extract(days from
                            (date_year || '-01-01')::date + interval '1 year' - (date_year || '-01-01')::date
                        )::numeric as max_dates_cnt,
                    extract(days from
                            least(now(), (date_year || '-01-01')::date + interval '1 year') -
                            greatest(min_date, (date_year || '-01-01')::date)
                        )::numeric as total_dates_cnt
             from (
                      select *,
                             extract(year from date) as date_year
                      from (
                               select collection_uniq_id,
                                      min_date,
                                      generate_series(min_date, max_date, interval '1 day')::date date
                               from collection_symbol_stats
                           ) t
                  ) t
     ),
     fee_schedule as
         (
             select collection_uniq_id, date, fee
             from (
                      select collection_uniq_id,
                             date,
                             is_holiday,
                             sum(fee) over (partition by collection_uniq_id, grp) as fee
                      from (
                               select *,
                                      {{ var('billing_value_fee_multiplier') }}::numeric * total_dates_cnt /
                                      max_dates_cnt / trading_dates_cnt                         as fee,
                                      t.date is null                                            as is_holiday,
                                      sum(case when t.date is not null then 1 end)
                                      over (partition by collection_uniq_id order by date desc) as grp
                               from schedule
                                        left join (
                                                      select collection_uniq_id, date
                                                      from {{ ref('collection_ticker_weights') }}
                                                      group by collection_uniq_id, date
                                                  ) t using (collection_uniq_id, date)
                                        left join (
                                                      select collection_uniq_id, date_year, count(*) as trading_dates_cnt
                                                      from schedule
                                                      group by collection_uniq_id, date_year
                                                  ) annual_stats using (collection_uniq_id, date_year)
                           ) t
                  ) t
             where not is_holiday
     ),
     daily_collection_gain_with_fee as
         (
             select profile_id,
                    collection_id,
                    collection_uniq_id,
                    date,
                    daily_collection_gain * (1 - fee) as daily_collection_gain,
                    dividends_value,
                    updated_at
             from daily_collection_gain
                      join fee_schedule using (collection_uniq_id, date)
     ),
     daily_collection_gain_cumulative as
         (
             select profile_id,
                    collection_id,
                    collection_uniq_id,
                    date,
                    exp(sum(ln(daily_collection_gain))
                        over (partition by collection_uniq_id order by date)) as cumulative_daily_relative_gain,
                    dividends_value,
                    updated_at
             from daily_collection_gain_with_fee
     )
select daily_collection_gain_cumulative.profile_id,
       daily_collection_gain_cumulative.collection_id,
       collection_uniq_id,
       date,
       date_trunc('week', date)::date              as date_week,
       date_trunc('month', date)::date             as date_month,
       coalesce(cumulative_daily_relative_gain, 1) as value,
       daily_collection_gain_cumulative.dividends_value,
       collection_uniq_id || '_' || date           as id,
       daily_collection_gain_cumulative.updated_at
from daily_collection_gain_cumulative

{% if is_incremental() %}
         left join {{ this }} old_data using (collection_uniq_id, date)
where old_data.collection_uniq_id is null
   or abs(coalesce(cumulative_daily_relative_gain, 1) - old_data.value) > {{ var('price_precision') }}
{% endif %}
