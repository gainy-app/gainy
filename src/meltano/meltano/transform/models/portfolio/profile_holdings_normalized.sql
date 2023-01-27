{{
  config(
    materialized = "view",
  )
}}


select *
from {{ ref('profile_holdings_normalized_all') }}
where not is_hidden
