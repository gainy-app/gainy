{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}

select profile_id,
       ticker_symbol as symbol,
       sum(profile_holdings_normalized.quantity) as quantity
from {{ ref('profile_holdings_normalized') }}
group by profile_id, ticker_symbol