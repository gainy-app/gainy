{{
  config(
    materialized = "view",
  )
}}


select profile_holdings_normalized_dynamic.profile_id,
       profile_holdings_normalized_dynamic.symbol,
       value_to_portfolio_value,
       actual_value,
       relative_gain_1d,
       absolute_gain_1d,
       relative_gain_total,
       absolute_gain_total
from {{ ref('profile_holdings_normalized_dynamic') }}
         join {{ ref('portfolio_holding_gains') }} using (holding_id_v2)
where profile_holdings_normalized_dynamic.is_app_trading
  and profile_holdings_normalized_dynamic.collection_id is null
