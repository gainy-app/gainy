{{
  config(
    materialized = "view",
  )
}}

select t.*,
       (actual_value / (1e-9 + sum(actual_value) over (partition by profile_id)))::double precision as value_to_portfolio_value
from (
         select plaid_holding_gains.profile_id,
                holding_group_id,
                max(plaid_holding_gains.updated_at)                     as updated_at,
                sum(actual_value)                                           as actual_value,
                sum(ltt_quantity_total)                                     as ltt_quantity_total,
                sum(absolute_gain_1d)                                       as absolute_gain_1d,
                sum(absolute_gain_1w)                                       as absolute_gain_1w,
                sum(absolute_gain_1m)                                       as absolute_gain_1m,
                sum(absolute_gain_3m)                                       as absolute_gain_3m,
                sum(absolute_gain_1y)                                       as absolute_gain_1y,
                sum(absolute_gain_5y)                                       as absolute_gain_5y,
                sum(absolute_gain_total)                                    as absolute_gain_total,
                sum(actual_value * relative_gain_1d) / sum(actual_value)    as relative_gain_1d,
                sum(actual_value * relative_gain_1w) / sum(actual_value)    as relative_gain_1w,
                sum(actual_value * relative_gain_1m) / sum(actual_value)    as relative_gain_1m,
                sum(actual_value * relative_gain_3m) / sum(actual_value)    as relative_gain_3m,
                sum(actual_value * relative_gain_1y) / sum(actual_value)    as relative_gain_1y,
                sum(actual_value * relative_gain_5y) / sum(actual_value)    as relative_gain_5y,
                sum(actual_value * relative_gain_total) / sum(actual_value) as relative_gain_total
         from {{ ref('plaid_holding_gains') }}
                  join {{ ref('profile_holdings_normalized') }} using (holding_id_v2)
         where holding_group_id is not null
         group by plaid_holding_gains.profile_id, holding_group_id
         having sum(actual_value) > 0

         union all

         select profile_id,
                holding_group_id,
                updated_at,
                actual_value,
                ltt_quantity_total,
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
                relative_gain_total
         from {{ ref('drivewealth_portfolio_holding_group_gains') }}
    ) t
