{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}


with account_stats as
         (
             select profile_id,
                    sum(cash_balance)                  as cash_balance,
                    sum(cash_available_for_withdrawal) as cash_available_for_withdrawal
             from (
                      select distinct on (
                          profile_id, drivewealth_accounts.ref_id
                          ) profile_id,
                            drivewealth_accounts_money.cash_balance,
                            drivewealth_accounts_money.cash_available_for_withdrawal

                      from {{ source('app', 'drivewealth_accounts_money') }}
                               join {{ source('app', 'drivewealth_accounts') }}
                                    on drivewealth_accounts.ref_id = drivewealth_accounts_money.drivewealth_account_id
                               join {{ source('app', 'drivewealth_users') }}
                                    on drivewealth_users.ref_id = drivewealth_accounts.drivewealth_user_id
                      order by profile_id, drivewealth_accounts.ref_id, drivewealth_accounts_money.created_at desc
                  ) t
             group by profile_id
         ),
    deposit_stats as
         (
             select profile_id,
                    sum(amount) as pending_cash
             from {{ source('app', 'trading_money_flow') }}
             where status = 'PENDING' and amount > 0
             group by profile_id
         ),
     portfolio_stats as
         (
             select profile_id,
                    sum(cash_value) as cash_value
             from (
                      select distinct on (
                          profile_id, drivewealth_portfolios.ref_id
                          ) profile_id,
                            drivewealth_portfolio_statuses.cash_value
                      from {{ source('app', 'drivewealth_portfolio_statuses') }}
                               join {{ source('app', 'drivewealth_portfolios') }}
                                    on drivewealth_portfolios.ref_id =
                                       drivewealth_portfolio_statuses.drivewealth_portfolio_id
                      order by profile_id, drivewealth_portfolios.ref_id, drivewealth_portfolio_statuses.created_at desc
                  ) t
             group by profile_id
     ),
     trading_collection_versions_stats as
         (
             select profile_id,
                    sum(target_amount_delta) as target_amount_delta
             from {{ source('app', 'trading_collection_versions') }}
             where status in ('PENDING', 'PENDING_EXECUTION')
             group by profile_id
     )
select profile_id,
       trading_accounts.account_no,
       kyc_status.status = 'APPROVED'                  as kyc_done,
       kyc_status.status                               as kyc_status,
       kyc_status.message                              as kyc_message,
       kyc_status.error_messages                       as kyc_error_messages,
       trading_funding_accounts.profile_id is not null as funding_account_connected,
       trading_money_flow.profile_id is not null       as deposited_funds,
       coalesce(pending_cash, 0)::double precision     as pending_cash,
       coalesce(
{% if var("drivewealth_is_uat") %}
               account_stats.cash_balance
{% else %}
               cash_available_for_withdrawal
{% endif %}
           , 0)::double precision                      as withdrawable_cash,
       coalesce(
                   portfolio_stats.cash_value + coalesce(target_amount_delta, 0),
{% if var("drivewealth_is_uat") %}
                   account_stats.cash_balance
{% else %}
                   cash_available_for_withdrawal
{% endif %}
           , 0)::double precision                      as buying_power
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
                       select distinct profile_id
                       from {{ source('app', 'trading_funding_accounts') }}
                   ) trading_funding_accounts using (profile_id)
         left join (
                       select distinct profile_id
                       from {{ source('app', 'trading_money_flow') }}
                       where status = 'SUCCESS'
                   ) trading_money_flow using (profile_id)
         left join (
                       select distinct on (profile_id) profile_id, ref_no as account_no
                       from {{ source('app', 'drivewealth_accounts') }}
                       join {{ source('app', 'drivewealth_users') }}
                            on drivewealth_accounts.drivewealth_user_id = drivewealth_users.ref_id
                       where drivewealth_accounts.status = 'OPEN'
                   ) trading_accounts using (profile_id)
         left join deposit_stats using (profile_id)
         left join account_stats using (profile_id)
         left join portfolio_stats using (profile_id)
         left join trading_collection_versions_stats using (profile_id)
