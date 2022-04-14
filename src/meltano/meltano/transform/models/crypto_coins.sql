{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    post_hook=[
      index(this, 'symbol', true),
    ]
  )
}}

select distinct on (lower(symbol)) *
from {{ source('coingecko', 'coingecko_coin') }}
order by lower(symbol), coingecko_rank
