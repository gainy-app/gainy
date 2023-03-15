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
             from daily_collection_gain
         ),
     annual_stats as
         (
             select symbol,
                    date_year,
                    count(date) as trading_dates_cnt
             from {{ ref('historical_prices') }}
             group by symbol, date_year
         ),
     collection_symbol_stats as
         (
             select collection_uniq_id, symbol, min(date) as min_date
             from {{ ref('collection_ticker_weights') }}
             group by collection_uniq_id, symbol
     ),
     fee_schedule as
         (
             select collection_uniq_id,
                    symbol,
                    date, {{ var('annual_fee') }}::numeric * total_dates_cnt / max_dates_cnt / trading_dates_cnt as fee_pct
             from (
                      select collection_uniq_id,
                             symbol,
                             date,
                             trading_dates_cnt::numeric,
                             extract(days from
                                     (date_year || '-01-01')::date + interval '1 year' - (date_year || '-01-01')::date
                                 )::numeric as max_dates_cnt,
                             extract(days from
                                     least(now(), (date_year || '-01-01')::date + interval '1 year') -
                                     greatest(min_date, (date_year || '-01-01')::date)
                                 )::numeric as total_dates_cnt
                      from {{ ref('collection_ticker_weights') }}
                               join {{ ref('historical_prices') }} using (symbol, date)
                               join annual_stats using (symbol, date_year)
                               join collection_symbol_stats using (collection_uniq_id, symbol)
                  ) t
     ),
     fees_cumulative as
         (
             select collection_uniq_id,
                    date,
                    sum(fee_pct) over wnd as cumulative_fee_pct
             from fee_schedule
                 window wnd as (partition by collection_uniq_id order by date)
         )
select daily_collection_gain_cumulative.profile_id,
       daily_collection_gain_cumulative.collection_id,
       collection_uniq_id,
       date,
       date_trunc('week', date)::date                                   as date_week,
       date_trunc('month', date)::date                                  as date_month,
       coalesce(cumulative_daily_relative_gain, 1)                      as value,
       coalesce(cumulative_daily_relative_gain, 1) - cumulative_fee_pct as value,
       daily_collection_gain_cumulative.dividends_value,
       collection_uniq_id || '_' || date                                as id,
       daily_collection_gain_cumulative.updated_at
from daily_collection_gain_cumulative
         join fees_cumulative using (collection_uniq_id, date)

{% if is_incremental() %}
         left join {{ this }} old_data using (collection_uniq_id, date)
where old_data.collection_uniq_id is null
   or abs(coalesce(cumulative_daily_relative_gain, 1) - old_data.value) > 1e-6
{% endif %}
