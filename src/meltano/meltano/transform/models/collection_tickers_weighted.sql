{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}

with all_ticker_collections as
         (
              select profile_id, (profile_id || '_' || collection_id)::varchar as uniq_id, collection_id, symbol
              from {{ source('app', 'personalized_ticker_collections') }}
              union all
              select null, ('0_' || collection_id)::varchar as uniq_id, collection_id, symbol
              from {{ ref('ticker_collections') }}
         )
select all_ticker_collections.profile_id,
       all_ticker_collections.collection_id,
       all_ticker_collections.uniq_id                     as collection_uniq_id,
       symbol,
       ticker_metrics.market_capitalization::numeric /
       sum(ticker_metrics.market_capitalization::numeric)
       over (partition by all_ticker_collections.uniq_id) as weight
from all_ticker_collections
    join {{ ref ('ticker_metrics') }} using (symbol)
where ticker_metrics.market_capitalization is not null
