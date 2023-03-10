{{
  config(
    materialized = "view",
  )
}}

select distinct on (profile_id) *
from {{ ref('drivewealth_portfolio_gains_raw') }}
order by profile_id desc, updated_at desc
