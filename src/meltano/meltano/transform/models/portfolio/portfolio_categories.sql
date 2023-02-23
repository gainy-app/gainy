{{
  config(
    materialized = "view",
  )
}}


select distinct profile_id, category_id
from {{ ref('profile_holdings_normalized') }}
         join {{ ref('ticker_categories') }} on ticker_categories.symbol = profile_holdings_normalized.ticker_symbol
where collection_id is null

union distinct

select distinct profile_id, category_id
from {{ ref('profile_holdings_normalized') }}
         join {{ ref('collection_categories') }} using (collection_id)
where collection_id is not null
