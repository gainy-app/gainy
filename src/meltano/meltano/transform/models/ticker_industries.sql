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
             select *
             from {{ ref('tickers') }}
             where type = 'common stock'
         )

select concat(common_stocks.symbol, '_', gainy_industries.id)::varchar as id,
       common_stocks.symbol,
       gainy_industries.id as industry_id,
       1 as similarity,
       1 as industry_order,
       now()::timestamp as updated_at
from common_stocks
join {{ ref('gainy_industries') }} on gainy_industries.name = common_stocks.gic_sub_industry
