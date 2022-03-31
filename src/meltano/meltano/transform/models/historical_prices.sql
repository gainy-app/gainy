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
SELECT rhp.code,
       CONCAT(rhp.code, '_', rhp.date)::varchar as id,
       rhp.adjusted_close,
       rhp.close,
       rhp.date::date,
       date_trunc('week', rhp.date::date) as date_week,
       date_trunc('month', rhp.date::date) as date_month,
       rhp.high,
       rhp.low,
       rhp.open,
       rhp.volume
from {{ source('eod', 'eod_historical_prices') }} rhp
{% if is_incremental() %}
    left join max_updated_at on rhp.code = max_updated_at.code
    where rhp.date::date >= max_updated_at.max_date or max_updated_at.max_date is null
{% endif %}
