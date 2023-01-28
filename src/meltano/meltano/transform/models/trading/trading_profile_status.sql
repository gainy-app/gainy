{{
  config(
    materialized = "view",
  )
}}


with account_stats as
         (
             select profile_id,
                    bool_or(deposited_funds)   as deposited_funds,
                    sum(pending_cash)          as pending_cash,
                    sum(pending_orders_amount) as pending_orders_amount,
                    sum(pending_orders_sum)    as pending_orders_sum,
                    sum(withdrawable_cash)     as withdrawable_cash,
                    sum(buying_power)          as buying_power,
                    max(account_no)::varchar   as account_no,
                    max(updated_at)            as updated_at
             from {{ ref('trading_account_status') }}
             group by profile_id
         )
select profile_id,
       account_no,
       kyc_status.status is not null and kyc_status.status = 'APPROVED' as kyc_done,
       kyc_status.status                                                as kyc_status,
       kyc_status.message                                               as kyc_message,
       kyc_status.error_messages                                        as kyc_error_messages,
       trading_funding_accounts.profile_id is not null                  as funding_account_connected,
       coalesce(deposited_funds, false)                                 as deposited_funds,
       coalesce(pending_cash, 0)::double precision                      as pending_cash,
       pending_orders_amount,
       pending_orders_sum,
       coalesce(withdrawable_cash, 0)::double precision                 as withdrawable_cash,
       coalesce(buying_power, 0)::double precision                      as buying_power,
       greatest(kyc_status.created_at,
           trading_funding_accounts.updated_at,
           account_stats.updated_at)::timestamp                         as updated_at
from (
         select id as profile_id
         from {{ source('app', 'profiles') }}
     ) profiles
         left join (
                       select distinct on (profile_id) *
                       from {{ source('app', 'kyc_statuses') }}
                       order by profile_id, created_at desc
                   ) kyc_status using (profile_id)
         left join (
                       select profile_id, max(updated_at) as updated_at
                       from {{ source('app', 'trading_funding_accounts') }}
                       group by profile_id
                   ) trading_funding_accounts using (profile_id)
         left join account_stats using (profile_id)
