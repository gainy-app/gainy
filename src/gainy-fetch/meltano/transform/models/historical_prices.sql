{{
  config(
    materialized = "table",
    post_hook=[
      'create unique index if not exists {{ get_index_name(this, "code__date") }} (code, date)',
      fk(this, 'code', this.schema, 'tickers', 'symbol'),
    ]
  )
}}

SELECT rhp.code,
       rhp.adjusted_close,
       rhp.close,
       rhp.date::date,
       rhp.high,
       rhp.low,
       rhp.open,
       rhp.volume
from {{ source('eod', 'eod_historical_prices') }} rhp
join {{ ref('tickers') }} t ON t.symbol = rhp.code