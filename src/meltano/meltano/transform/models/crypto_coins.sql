{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    post_hook=[
      index(this, 'symbol', true),
    ]
  )
}}

select distinct on (symbol) *
from {{ source('coingecko', 'coingecko_coin') }}
order by symbol, coingecko_rank
