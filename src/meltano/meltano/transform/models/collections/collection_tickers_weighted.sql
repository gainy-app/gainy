{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}


select '0_' || collection_id as collection_uniq_id,
       symbol,
       now()::date           as date,
       null                  as profile_id,
       collection_id,
       weight::numeric
from {{ ref('ticker_collections') }}

union all

select profile_id || '_' || collection_id                       as collection_uniq_id,
       symbol,
       now()::date                                              as date,
       profile_id,
       collection_id,
       (ticker_metrics.market_capitalization /
        sum(ticker_metrics.market_capitalization)
        over (partition by profile_id, collection_id))::numeric as weight
from {{ source('app', 'personalized_ticker_collections') }}
         join {{ ref('ticker_metrics') }} using (symbol)
where ticker_metrics.market_capitalization is not null
