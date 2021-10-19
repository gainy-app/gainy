{{
  config(
    materialized = "view"
  )
}}


select ptc.profile_id, ptc.collection_id, ptc.symbol
from {{ source('app', 'personalized_ticker_collections') }} ptc
union
select null::integer as profile_id, tc.collection_id, tc.symbol
from {{ ref('ticker_collections') }} tc