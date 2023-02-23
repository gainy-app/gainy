{{
  config(
    materialized = "view",
  )
}}


select distinct profile_id, interest_id
from {{ ref('profile_holdings_normalized') }}
         join {{ ref('ticker_interests') }} on ticker_interests.symbol = profile_holdings_normalized.ticker_symbol
where collection_id is null

union distinct

select distinct profile_id, interest_id
from {{ ref('profile_holdings_normalized') }}
         join {{ ref('collection_interests') }} using (collection_id)
where collection_id is not null
