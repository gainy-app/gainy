{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}


with all_ticker_collections as
         (
             select profile_id, collection_id, (profile_id || '_' || collection_id)::varchar AS collection_uniq_id, symbol, null as weight
             from {{ source('app', 'personalized_ticker_collections') }}
             union all
             select null, collection_id, ('0_' || collection_id)::varchar AS collection_uniq_id, symbol, weight
             from {{ ref('ticker_collections') }}
         ),
    weighted_ticker_collections as
         (
             select collection_id, sum(weight) as weight_sum
             from {{ ref('ticker_collections') }}
             group by collection_id
         )
select now()::date                            as date,
       all_ticker_collections.profile_id,
       all_ticker_collections.collection_id,
       collection_uniq_id,
       symbol,
       case
           when weighted_ticker_collections.weight_sum is null or weighted_ticker_collections.weight_sum < 1e-3
               then
                   ticker_metrics.market_capitalization /
                   sum(ticker_metrics.market_capitalization)
                   over (partition by collection_uniq_id)
           else
               weight / weighted_ticker_collections.weight_sum
           end as weight
from all_ticker_collections
         join {{ ref('ticker_metrics') }} using (symbol)
         join weighted_ticker_collections using (collection_id)
where ticker_metrics.market_capitalization is not null
