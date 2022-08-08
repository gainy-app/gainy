{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('symbol, interest_id'),
      index(this, 'id', true),
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
         from {{ ref('ticker_industries') }} tind
                  join {{ ref('interest_industries') }} ii on ii.industry_id = tind.industry_id
         group by tind.symbol, ii.interest_id
     ),

     simple_ticker_interests as (
         select tis.symbol,
                tis.interest_id,
                2.*(tis.similarity-0.5) as sim_dif -- [-1..1]
         from ticker_interest_similarity tis
     ),
     complex_ticker_interests as
         (
             select simple_ticker_interests.interest_id,
                    ticker_components.symbol,
                    sum(simple_ticker_interests.sim_dif * component_weight) / sum(component_weight) as sim_dif
             from {{ ref('ticker_components') }}
                      join simple_ticker_interests
                           on simple_ticker_interests.symbol = ticker_components.component_symbol
             group by simple_ticker_interests.interest_id, ticker_components.symbol
             having sum(component_weight) > 0
         )

select (simple_ticker_interests.symbol || '_' || simple_ticker_interests.interest_id) as id,
       simple_ticker_interests.symbol,
       simple_ticker_interests.interest_id,
       simple_ticker_interests.sim_dif,
       now()::timestamp                                                               as updated_at
from simple_ticker_interests
    left join complex_ticker_interests using (symbol)
where complex_ticker_interests.symbol is null

union all

select (symbol || '_' || interest_id) as id,
       symbol,
       interest_id,
       sim_dif,
       now()::timestamp               as updated_at
from complex_ticker_interests
