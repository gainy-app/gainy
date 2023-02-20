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
                absolute_gain_1d,
                absolute_gain_1w,
                absolute_gain_1m,
                absolute_gain_3m,
                absolute_gain_1y,
                absolute_gain_5y,
                absolute_gain_total,
                relative_gain_1d,
                relative_gain_1w,
                relative_gain_1m,
                relative_gain_3m,
                relative_gain_1y,
                relative_gain_5y,
                relative_gain_total,
                ltt_quantity_total,
                plaid_holding_gains.updated_at
         from {{ ref('plaid_holding_gains') }}
                  join {{ ref('profile_holdings_normalized_all') }} using (holding_id_v2)
         where not profile_holdings_normalized_all.is_hidden

         union all

         select profile_id,
                holding_id_v2,
                actual_value,
                absolute_gain_1d,
                absolute_gain_1w,
                absolute_gain_1m,
                absolute_gain_3m,
                absolute_gain_1y,
                absolute_gain_5y,
                absolute_gain_total,
                relative_gain_1d,
                relative_gain_1w,
                relative_gain_1m,
                relative_gain_3m,
                relative_gain_1y,
                relative_gain_5y,
                relative_gain_total,
                ltt_quantity_total,
                updated_at::timestamp
         from {{ ref('drivewealth_portfolio_holding_gains') }}
     ) t
