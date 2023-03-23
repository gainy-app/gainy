{{
  config(
    materialized = "view",
  )
}}


select profile_id,
       profiles.created_at                 as sign_up_at,
       first_name,
       last_name,
       profile_scoring_settings.created_at as questionaire_completed_at,
       kyc_status,
       last_deposit_at,
       total_deposit_amount,
       actual_value                        as account_balance,
       buying_power,
       open_ttf_positions,
       absolute_gain_total,
       relative_gain_total,
       greatest(profiles.created_at,
           profile_scoring_settings.created_at,
           trading_profile_status.updated_at,
           portfolio_gains.updated_at,
           last_deposit_at)                as updated_at
from (
         select id as profile_id, first_name, last_name, created_at
         from {{ source('app', 'profiles') }}
     ) profiles
         left join {{ source('app', 'profile_scoring_settings') }} using (profile_id)
         left join {{ ref('trading_profile_status') }} using (profile_id)
         left join {{ ref('portfolio_gains') }} using (profile_id)
         left join (
                       select profile_id, count(*) filter ( where collection_id is not null ) as open_ttf_positions
                       from {{ ref('profile_holding_groups') }}
                       group by profile_id
                   ) open_positions using (profile_id)
         left join (
                       select profile_id, max(created_at) as last_deposit_at, sum(amount) as total_deposit_amount
                       from {{ source('app', 'trading_money_flow') }}
                       where amount > 0
                       group by profile_id
                   ) deposit_stats using (profile_id)
