{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('symbol, datetime'),
      index(this, 'id', true),
      'create index if not exists "datetime__symbol" ON {{ this }} (datetime, symbol)',
    ]
  )
}}


with data as materialized
         (
             select symbol,
                    date_month,
                    mode() within group ( order by date )      as open_date,
                    mode() within group ( order by date desc ) as close_date,
                    max(high)                                  as high,
                    min(low)                                   as low,
                    sum(volume)                                as volume
             from {{ ref('historical_prices') }}
             group by symbol, date_month
         )
select data.symbol || '_' || data.date_month as id,
       data.symbol                           as symbol,
       data.date_month                       as datetime,
       hp_open.open,
       data.high,
       data.low,
       hp_close.close,
       hp_close.adjusted_close,
       hp_close.updated_at,
       data.volume
from data
         join {{ ref('historical_prices') }} hp_open on hp_open.symbol = data.symbol and hp_open.date = data.open_date
         join {{ ref('historical_prices') }} hp_close on hp_close.symbol = data.symbol and hp_close.date = data.close_date
{% if is_incremental() %}
         left join {{ this }} old_data on old_data.symbol = data.symbol and old_data.datetime = data.date_month
where old_data.symbol is null -- no old data
   or (hp_close.updated_at is not null and old_data.updated_at is null) -- old data is null and new is not
   or hp_close.updated_at > old_data.updated_at -- new data is newer than the old one
{% endif %}

-- OK created incremental model historical_prices_aggregated_1m  SELECT 1914573 in 225.34s
-- OK created incremental model historical_prices_aggregated_1m  SELECT 1914573 in 367.38s
