{{
  config(
    materialized = "view",
  )
}}


with account_stats as
         (
             select trading_account_id,
                    sum(t.cash_balance)                  as cash_balance,
                    sum(t.cash_available_for_trade)      as cash_available_for_trade,
                    sum(t.cash_available_for_withdrawal) as cash_available_for_withdrawal,
                    max(t.created_at)                    as updated_at
             from (
                      select distinct on (
                          drivewealth_account_id
                          ) drivewealth_account_id,
                            cash_balance,
                            cash_available_for_trade,
                            cash_available_for_withdrawal,
                            created_at
                      from {{ source('app', 'drivewealth_accounts_money') }}
                      order by drivewealth_account_id, created_at desc
                  ) t
                      join {{ source('app', 'drivewealth_accounts') }}
                           on drivewealth_accounts.ref_id = t.drivewealth_account_id
             group by trading_account_id
         ),
     deposit_stats as
         (
             select trading_account_id,
                    sum(amount) as pending_cash
             from {{ source('app', 'trading_money_flow') }}
             where status in ('PENDING', 'APPROVED')
               and amount > 0
             group by trading_account_id
     ),
     portfolio_stats as
         (
             select trading_account_id,
                    sum(cash_value)   as cash_value,
                    max(t.created_at) as updated_at
             from (
                      select distinct on (
                          drivewealth_portfolio_id
                          ) drivewealth_portfolio_id,
                            cash_value,
                            created_at
                      from {{ source('app', 'drivewealth_portfolio_statuses') }}
                      order by drivewealth_portfolio_id, created_at desc
                  ) t
                      join {{ source('app', 'drivewealth_portfolios') }}
                           on drivewealth_portfolios.ref_id = t.drivewealth_portfolio_id
                      join {{ source('app', 'drivewealth_accounts') }}
                           on drivewealth_accounts.ref_id = drivewealth_portfolios.drivewealth_account_id
             group by trading_account_id
     )
select profile_id,
       trading_account_id,
       trading_accounts.account_no,
       trading_money_flow.trading_account_id is not null as deposited_funds,
       coalesce(pending_cash, 0)::double precision       as pending_cash,
       coalesce(pending_order_stats.abs_amount_sum,
           0)::double precision                          as pending_orders_amount,
       coalesce(pending_order_stats.amount_sum,
           0)::double precision                          as pending_orders_sum,
       coalesce(
{% if var("drivewealth_is_uat") == "true" %}
               cash_available_for_trade
{% else %}
               cash_available_for_withdrawal
{% endif %}
           , 0)::double precision                        as withdrawable_cash,
       greatest(
           coalesce(portfolio_stats.cash_value, 0) + coalesce(pending_cash, 0) - coalesce(pending_order_stats.amount_sum, 0)
           , 0)::double precision                        as buying_power,
       greatest(trading_accounts.updated_at,
           trading_money_flow.updated_at,
           account_stats.updated_at,
           portfolio_stats.updated_at,
           pending_order_stats.updated_at)::timestamp    as updated_at
from (
         select distinct on (
             profile_id
             ) profile_id,
               ref_no as account_no,
               trading_account_id,
               drivewealth_accounts.updated_at
         from {{ source('app', 'drivewealth_accounts') }}
                  join {{ source('app', 'drivewealth_users') }}
                       on drivewealth_accounts.drivewealth_user_id = drivewealth_users.ref_id
         where drivewealth_accounts.status = 'OPEN'
     ) trading_accounts
         left join (
                       select trading_account_id, max(updated_at) as updated_at
                       from {{ source('app', 'trading_money_flow') }}
                       where status != 'FAILED'
                       group by trading_account_id
                   ) trading_money_flow using (trading_account_id)
         left join deposit_stats using (trading_account_id)
         left join account_stats using (trading_account_id)
         left join portfolio_stats using (trading_account_id)
         left join (
                       select profile_id,
                              sum(amount_sum) as amount_sum,
                              sum(abs_amount_sum) as abs_amount_sum,
                              max(updated_at) as updated_at
                       from (
                                select profile_id,
                                       t.amount_sum,
                                       t.abs_amount_sum,
                                       t.updated_at
                                from (
                                         -- todo link trading_collection_versions to trading_account
                                         select profile_id,
                                                collection_id,
                                                sum(target_amount_delta - coalesce(executed_amount, 0))      as amount_sum,
                                                sum(abs(target_amount_delta - coalesce(executed_amount, 0))) as abs_amount_sum,
                                                max(updated_at)                                              as updated_at
                                         from {{ source('app', 'trading_collection_versions') }}
                                         where status in ('PENDING_EXECUTION', 'PENDING')
                                         group by profile_id, collection_id
                                     ) t

                                union all

                                select profile_id,
                                       t.amount_sum,
                                       t.abs_amount_sum,
                                       t.updated_at
                                from (
                                         -- todo link trading_orders to trading_account
                                         select profile_id,
                                                symbol,
                                                sum(target_amount_delta - coalesce(executed_amount, 0))      as amount_sum,
                                                sum(abs(target_amount_delta - coalesce(executed_amount, 0))) as abs_amount_sum,
                                                max(updated_at)                                              as updated_at
                                         from {{ source('app', 'trading_orders') }}
                                         where status in ('PENDING_EXECUTION', 'PENDING')
                                         group by profile_id, symbol
                                     ) t
                            ) t
                       group by profile_id
                   ) pending_order_stats using (profile_id)
