{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists "symbol__interest_id" ON {{ this }} (symbol, interest_id)',
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}

 -- interests uses predefined per-interest lists of industries, so that tickers in ticker-interests here are in the same scope of ticker types that was enabled for making ticker-industries
with 

     ticker_interest_similarity as (
         select tind.symbol,
                ii.interest_id,
                max(tind.similarity) as similarity -- entrance of ticker to interest by biggest industry that is listed in interest
         from {{ ref ('ticker_industries') }} tind
                  join {{ ref('interest_industries') }} ii on ii.industry_id = tind.industry_id
         group by tind.symbol, ii.interest_id
     ),

     sim_dif as (
         select tis.symbol,
                tis.interest_id,
                2.*(tis.similarity-0.5) as sim_dif -- [-1..1]
         from ticker_interest_similarity tis
     )

select (sd.symbol || '_' || sd.interest_id)::varchar as id,
       sd.symbol,
       sd.interest_id,
       sd.sim_dif,
       now()::timestamp                              as updated_at
from sim_dif sd
