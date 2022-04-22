{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists "code__date" ON {{ this }} (code, date)',
    ]
  )
}}


{% if is_incremental() %}
with
     max_updated_at as (select code, max(date) as max_date from {{ this }} group by code)
{% endif %}
SELECT code,
       (code || '_' || date)::varchar as id,
       substr(date, 0, 8) date_month,
--        date_trunc('week', date::date),
       adjusted_close,
       close,
       date::date,
       high,
       low,
       open,
       volume
from {{ source('eod', 'eod_historical_prices') }}
{% if is_incremental() %}
    left join max_updated_at using (code)
    where date::date >= max_updated_at.max_date or max_updated_at.max_date is null
{% endif %}
