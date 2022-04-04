{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}


with all_ticker_collections as
         (
             select profile_id, collection_id, symbol
             from {{ source('app', 'personalized_ticker_collections') }}
             union all
             select null, collection_id, symbol
             from {{ ref('ticker_collections') }}
         )
select all_ticker_collections.profile_id,
       all_ticker_collections.collection_id,
       profile_collections.uniq_id                     as collection_uniq_id,
       symbol,
       ticker_metrics.market_capitalization::numeric /
       sum(ticker_metrics.market_capitalization::numeric)
       over (partition by profile_collections.uniq_id) as weight
from all_ticker_collections
         join {{ ref('ticker_metrics') }} using (symbol)
         join {{ ref('profile_collections') }}
              on profile_collections.id = all_ticker_collections.collection_id
                  and (all_ticker_collections.profile_id is null or
                       profile_collections.profile_id = all_ticker_collections.profile_id)
where ticker_metrics.market_capitalization is not null
