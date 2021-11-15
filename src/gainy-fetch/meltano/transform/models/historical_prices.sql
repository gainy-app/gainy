{{
  config(
    materialized = "incremental",
    unique_key = "id",
    incremental_strategy = 'insert_overwrite',
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists {{ get_index_name(this, "code__date") }} (code, date)',
    ]
  )
}}


{% if is_incremental() %}
with
    max_updated_at as (select max(date) as max_date from {{ this }})
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
join {{ ref('tickers') }} t ON t.symbol = rhp.code
{% if is_incremental() %}
    join max_updated_at on true
    where date::date >= max_updated_at.max_date
{% endif %}
