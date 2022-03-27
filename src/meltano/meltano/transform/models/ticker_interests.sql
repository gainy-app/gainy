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

with common_stocks as
         (
             select *
             from {{ ref('tickers') }}
             where "type" = 'common stock'
         ),

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
                ((1e-30 + (similarity - (min(similarity) over (partition by tis.interest_id))))
                     / (1e-30 + (max(similarity) over (partition by tis.interest_id) -
                                 min(similarity) over (partition by tis.interest_id)))
                    + 1e-2) /
                (1. + 1e-2) as sim_dif -- [1e-2 .. [0.. 1-1e-2]] (inside-each-interest "world" normalized + 1e-2)
         from ticker_interest_similarity tis
     )

select (sd.symbol || '_' || sd.interest_id)::varchar as id,
       sd.symbol,
       sd.interest_id,
       sd.sim_dif,
       now()::timestamp                              as updated_at
from sim_dif sd
         join common_stocks using (symbol)
