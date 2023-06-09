with deposit_stats as
         (
             select profile_id,
                    sum(amount) as deposited_amount
             from {{ source('app', 'trading_money_flow') }}
                      join {{ ref('trading_profile_status') }} using (profile_id)
             where status = 'SUCCESS'
               and amount > 0
               and created_at < kyc_passed_at + interval '4 weeks'
             group by profile_id
         )
select from_profile_id                                  as profile_id,
       to_profile_id                                    as invited_profile_id,
       invitations.created_at                           as invited_at,
       profiles.first_name || ' ' || profiles.last_name as name,
       profiles.created_at is not null                  as step1_signed_up,
       coalesce(kyc_done, false)                        as step2_brokerate_account_open,
       coalesce(deposited_amount, 0) >= 500             as step3_deposited_enough,
       coalesce(deposited_amount, 0) >= 500             as is_complete
from {{ source('app', 'invitations') }}
         left join {{ source('app', 'profiles') }} on invitations.to_profile_id = profiles.id
         left join {{ ref('trading_profile_status') }} on trading_profile_status.profile_id = profiles.id
         left join deposit_stats using (profile_id)
