{{
  config(
    materialized = "view",
  )
}}


select *
from {{ ref('profile_holdings_normalized_all') }}
where abs(quantity) > 0
