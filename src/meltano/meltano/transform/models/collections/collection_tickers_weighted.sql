{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}


select now()::date           as date,
       null                  as profile_id,
       collection_id,
       '0_' || collection_id as collection_uniq_id,
       symbol,
       weight
from {{ ref('static_collection_tickers_weighted') }}

union all

select now()::date                        as date,
       profile_id,
       collection_id,
       profile_id || '_' || collection_id as collection_uniq_id,
       symbol,
       (
               ticker_metrics.market_capitalization /
               sum(ticker_metrics.market_capitalization) over (partition by profile_id, collection_id)
           )::double precision            as weight
from {{ source('app', 'personalized_ticker_collections') }}
         join {{ ref('ticker_metrics') }} using (symbol)
where ticker_metrics.market_capitalization is not null
