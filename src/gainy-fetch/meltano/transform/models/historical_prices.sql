{{
  config(
    materialized = "table",
    post_hook=[
      'create unique index if not exists {{ get_index_name(this, "code__date") }} (code, date)',
      fk(this, 'code', 'tickers', 'symbol'),
    ]
  )
}}

SELECT code,
       adjusted_close,
       close,
       date::date,
       high,
       low,
       open,
       volume
from raw_historical_prices