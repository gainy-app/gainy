{{
  config(
    materialized = "table",
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists {{ get_index_name(this, "code__date") }} (code, date)',
    ]
  )
}}

/*
TODO:incremental_model
    materialized = "incremental",
    unique_key = "id",
    incremental_strategy = 'insert_overwrite',
 */

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