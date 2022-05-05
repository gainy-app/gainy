{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('symbol, industry_id'),
      index(this, 'id', true),
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}

with common_stocks as
         (
             select symbol, gic_sub_industry
             from {{ ref('tickers') }}
             where type = 'common stock'
         ),
    crypto_stocks as
         (
             select symbol
             from {{ ref('tickers') }}
             where type = 'crypto'
         )

select concat(common_stocks.symbol, '_', gainy_industries.id)::varchar as id,
       common_stocks.symbol,
       gainy_industries.id as industry_id,
       1. as similarity,
       1 as industry_order,
       now()::timestamp as updated_at
from common_stocks
join {{ ref('gainy_industries') }} on gainy_industries.name = common_stocks.gic_sub_industry

union all

select concat(crypto_stocks.symbol, '_', gainy_industries.id)::varchar as id,
       crypto_stocks.symbol,
       gainy_industries.id as industry_id,
       1. as similarity,
       1 as industry_order,
       now()::timestamp as updated_at
from crypto_stocks
         join {{ source('gainy', 'crypto_ticker_industries') }} on crypto_ticker_industries.cc_symbol = crypto_stocks.symbol
         join {{ ref('gainy_industries') }} on gainy_industries.name = crypto_ticker_industries.industry
