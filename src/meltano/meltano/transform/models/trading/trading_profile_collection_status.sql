{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}


select profile_holding_groups.profile_id,
       profile_holding_groups.collection_id,
       value_to_portfolio_value,
       actual_value,
       relative_gain_1d,
       absolute_gain_1d,
       relative_gain_total,
       absolute_gain_total
from {{ ref('profile_holding_groups') }}
         join {{ ref('portfolio_holding_group_gains') }} on portfolio_holding_group_gains.holding_group_id = profile_holding_groups.id
where profile_holding_groups.collection_id is not null
