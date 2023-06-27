{{
  config(
    materialized = "view",
  )
}}


select profile_id,
       profiles.created_at                 as sign_up_at,
       email,
       first_name,
       last_name,
       user_type,
       profile_scoring_settings.created_at as questionaire_completed_at,
       kyc_status,
       kyc_start_date,
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
         select id as profile_id, email, first_name, last_name, created_at,
                case
                    when '{{ var('env') }}' != 'production'
                        then 'test'
                    when array_position(string_to_array('{{ var('gainy_employee_profile_ids') }}', ','), id::text) is not null
                      or array_position(string_to_array('{{ var('gainy_employee_emails') }}', ','), email::text) is not null
                        then 'gainy'
                    when email ilike '%gainy.app'
                        or email ilike '%test%'
                        or last_name ilike '%test%'
                        or first_name ilike '%test%'
                        then 'test'
                    else 'customer'
                    end as user_type
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
                         and status = 'SUCCESS'
                         and type = 'MANUAL'
                       group by profile_id
                   ) deposit_stats using (profile_id)
         left join (
                       select profile_id, min(created_at) as kyc_start_date from {{ source('app', 'kyc_statuses') }} group by profile_id
                   ) kyc_stats using (profile_id)
