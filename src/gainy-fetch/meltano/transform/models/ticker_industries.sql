{{
  config(
    materialized = "table",
    post_hook=[
      fk(this, 'symbol', this.schema, 'tickers', 'symbol'),
      fk(this, 'industry_id', this.schema, 'gainy_industries', 'id'),
      'create unique index if not exists {{ get_index_name(this, "industry_id__symbol") }} (industry_id, symbol)',
    ]
  )
}}

with manual_industries as (
    select code as symbol, cast (id as integer) as industry_id_0
    from {{ source('gainy', 'gainy_ticker_industries') }} gti
        join {{ ref('gainy_industries') }} gi
            on gti."industry name" = gi."name"
),
tickers_with_industries as (
    select ati.symbol, industry_id_0, industry_id_1, industry_id_2
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
    select symbol, industry_id from industries_1
    union
    select symbol, industry_id from industries_2
)
select ai.symbol, industry_id
from all_industries ai
         join {{ ref ('tickers') }} t
              on ai.symbol = t.symbol