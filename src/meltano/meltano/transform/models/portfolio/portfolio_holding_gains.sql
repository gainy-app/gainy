{{
  config(
    materialized = "view",
  )
}}


select t.*,
       (actual_value / (1e-9 + sum(actual_value) over (partition by profile_id)))::double precision as value_to_portfolio_value
from (
         select plaid_holding_gains.profile_id,
                holding_id_v2,
                actual_value,
                plaid_holding_gains.updated_at
         from {{ ref('plaid_holding_gains') }}
                  join {{ ref('profile_holdings_normalized_all') }} using (holding_id_v2)
         where not profile_holdings_normalized_all.is_hidden

         union all

         select profile_id,
                holding_id_v2,
                actual_value,
                now()::timestamp as updated_at
         from {{ ref('drivewealth_holdings') }}
     ) t
