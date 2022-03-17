{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists "industry_id__symbol" ON {{ this }} (industry_id, symbol)',
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}

with common_stocks as (
    select * from {{ ref('tickers') }} where type = 'common stock'
),
manual_industries as (
    select code as symbol, cast (id as integer) as industry_id_0
    from {{ source('gainy', 'gainy_ticker_industries') }} gti
        join {{ ref('gainy_industries') }} gi
            on gti."industry name" = gi."name"
),
tickers_with_industries as (
    select coalesce(ati.symbol, mi.symbol) as symbol, mi.industry_id_0, ati.industry_id_1, ati.industry_id_2
    from {{ source('gainy', 'auto_ticker_industries') }} ati
        full outer join manual_industries mi
            on ati.symbol = mi.symbol
),
industries_1 as (
    select symbol, coalesce (industry_id_0, industry_id_1) as industry_id
        from tickers_with_industries
),
industries_2 as (
    select
        symbol,
        case
            when industry_id_0 is null then industry_id_2
            when industry_id_0 = industry_id_1 then industry_id_2
            else industry_id_1
        end as industry_id
    from tickers_with_industries
),
all_industries as (
    select symbol, industry_id, 1 as industry_order from industries_1
        union
    select symbol, industry_id, 2 as industry_order from industries_2
)
select concat(ai.symbol, '_', industry_id)::varchar as id,
       ai.symbol,
       industry_id,
       industry_order,
       now() as updated_at
from all_industries ai
         join common_stocks cs
              on ai.symbol = cs.symbol
where ai.industry_id is not null