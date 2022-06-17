{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}


with all_ticker_collections as
         (
             select profile_id, collection_id, (profile_id || '_' || collection_id)::varchar AS collection_uniq_id, symbol
             from {{ source('app', 'personalized_ticker_collections') }}
             union all
             select null, collection_id, ('0_' || collection_id)::varchar AS collection_uniq_id, symbol
             from {{ ref('ticker_collections') }}
         )
select now()::date             as date,
       all_ticker_collections.profile_id,
       all_ticker_collections.collection_id,
       collection_uniq_id,
       symbol,
       (
               ticker_metrics.market_capitalization /
               sum(ticker_metrics.market_capitalization) over (partition by collection_uniq_id)
           )::double precision as weight
from all_ticker_collections
         join {{ ref('ticker_metrics') }} using (symbol)
where ticker_metrics.market_capitalization is not null
