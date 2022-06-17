{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('id'),
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}

with weighted_ticker_collections as
         (
             select collection_id, sum(weight) as weight_sum
             from {{ ref('ticker_collections') }}
             group by collection_id
         )
select collection_id || '_' || symbol as id,
       collection_id,
       symbol,
       (case
            when weighted_ticker_collections.weight_sum is null or weighted_ticker_collections.weight_sum < 1e-3
                then
                    ticker_metrics.market_capitalization /
                    sum(ticker_metrics.market_capitalization) over (partition by collection_id)
            else
                weight / weighted_ticker_collections.weight_sum
           end)::double precision     as weight,
       now()                          as updated_at
from {{ ref('ticker_collections') }}
         join {{ ref('ticker_metrics') }} using (symbol)
         join weighted_ticker_collections using (collection_id)
where ticker_metrics.market_capitalization is not null
