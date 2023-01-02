{{
  config(
    materialized = "view",
  )
}}


with account_stats as
         (
             select profile_id,
                    sum(cash_balance)                  as cash_balance,
                    sum(cash_available_for_trade)      as cash_available_for_trade,
                    sum(cash_available_for_withdrawal) as cash_available_for_withdrawal
             from (
                      select distinct on (
                          profile_id, drivewealth_accounts.ref_id
                          ) profile_id,
                            drivewealth_accounts_money.cash_balance,
                            drivewealth_accounts_money.cash_available_for_trade,
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
             where status in ('PENDING', 'APPROVED')
               and amount > 0
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
     )
select profile_id,
       trading_account_id,
       trading_accounts.account_no,
       trading_money_flow.trading_account_id is not null as deposited_funds,
       coalesce(pending_cash, 0)::double precision       as pending_cash,
       coalesce(
{% if var("drivewealth_is_uat") %}
               cash_available_for_trade
{% else %}
               cash_available_for_withdrawal
{% endif %}
           , 0)::double precision                        as withdrawable_cash,
       coalesce(
           coalesce(portfolio_stats.cash_value, 0) + coalesce(pending_cash, 0)
           , 0)::double precision                        as buying_power
from (
         select distinct on (
             profile_id
             ) profile_id,
               ref_no as account_no,
               trading_account_id
         from {{ source('app', 'drivewealth_accounts') }}
                  join {{ source('app', 'drivewealth_users') }}
                       on drivewealth_accounts.drivewealth_user_id = drivewealth_users.ref_id
         where drivewealth_accounts.status = 'OPEN'
     ) trading_accounts
         left join (
                       select distinct trading_account_id
                       from {{ source('app', 'trading_money_flow') }}
                       where status != 'FAILED'
                   ) trading_money_flow using (trading_account_id)
         left join deposit_stats using (profile_id)
         left join account_stats using (profile_id)
         left join portfolio_stats using (profile_id)
