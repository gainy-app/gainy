with deposit_stats as
         (
             select profile_id,
                    sum(amount) as deposited_amount
             from {{ source('app', 'trading_money_flow') }}
                      join {{ ref('trading_profile_status') }} using (profile_id)
             where status = 'SUCCESS'
               and amount > 0
               and created_at < kyc_passed_at + interval '4 weeks'
               and trading_money_flow.type = 'MANUAL'
             group by profile_id
         )
select from_profile_id                                     as profile_id,
       to_profile_id                                       as invited_profile_id,
       invitations.id                                      as invitation_id,
       invitations.created_at                              as invited_at,
       profiles.first_name || ' ' || profiles.last_name    as name,
       profiles.created_at is not null                     as step1_signed_up,
       coalesce(kyc_done, false)                           as step2_brokerate_account_open,
       coalesce(deposited_amount, 0) >= 500                as step3_deposited_enough,
       trading_money_flow.id is not null                   as is_payment_started,
       coalesce(trading_money_flow.status, '') = 'SUCCESS' as is_complete
from {{ source('app', 'invitations') }}
         left join {{ source('app', 'profiles') }} on invitations.to_profile_id = profiles.id
         left join {{ ref('trading_profile_status') }} on trading_profile_status.profile_id = profiles.id
         left join deposit_stats using (profile_id)
         left join app.invitation_cash_rewards
                  on invitation_cash_rewards.invitation_id = invitations.id
                      and invitation_cash_rewards.profile_id = from_profile_id
         left join app.trading_money_flow
                  on trading_money_flow.id = invitation_cash_rewards.money_flow_id
