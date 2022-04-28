{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('code, date'),
      index(this, 'id', true),
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
       rhp.high,
       rhp.low,
       rhp.open,
       rhp.volume
from {{ source('eod', 'eod_historical_prices') }} rhp
{% if is_incremental() %}
    left join max_updated_at on rhp.code = max_updated_at.code
    where rhp.date::date >= max_updated_at.max_date or max_updated_at.max_date is null
{% endif %}
