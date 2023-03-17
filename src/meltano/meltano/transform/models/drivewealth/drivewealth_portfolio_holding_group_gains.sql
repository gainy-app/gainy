{{
  config(
    materialized = "view",
  )
}}


select distinct on (
    profile_id,
    holding_group_id
    ) *
from {{ ref('drivewealth_portfolio_holding_group_gains_raw') }}
order by profile_id desc, holding_group_id desc, updated_at desc