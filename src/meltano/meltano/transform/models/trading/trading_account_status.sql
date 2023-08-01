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
                    count(id) filter ( where status != 'FAILED' )                    as not_failed_deposits_cnt,
                    count(id) filter ( where status = 'SUCCESS' )                    as successful_deposits_cnt,
                    sum(amount) filter ( where status not in ('FAILED', 'SUCCESS') ) as pending_cash,
                    max(updated_at) as updated_at
             from {{ source('app', 'trading_money_flow') }}
             where amount > 0
             group by trading_account_id
     ),
     portfolio_stats as
         (
             select trading_account_id,
                    sum(cash_value)   as cash_value,
                    max(t.created_at) as updated_at
             from (
                      select drivewealth_portfolio_id,
                             cash_value,
                             created_at
                      from {{ source('app', 'drivewealth_portfolio_statuses') }}
                               join (
                                        select drivewealth_portfolio_id, max(id) as id
                                        from {{ source('app', 'drivewealth_portfolio_statuses') }}
                                        group by drivewealth_portfolio_id
                                    ) t using (id, drivewealth_portfolio_id)
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
       deposit_stats.not_failed_deposits_cnt > 0      as deposited_funds,
       deposit_stats.successful_deposits_cnt > 0      as successfully_deposited_funds,
       coalesce(pending_cash, 0)::double precision    as pending_cash,
       coalesce(pending_order_stats.abs_amount_sum,
           0)::double precision                       as pending_orders_amount,
       coalesce(pending_order_stats.cnt, 0)::int      as pending_orders_count,
       coalesce(pending_order_stats.amount_sum,
           0)::double precision                       as pending_orders_sum,
       coalesce(
{% if var("drivewealth_is_uat") == "true" %}
               cash_available_for_trade
{% else %}
               cash_available_for_withdrawal
{% endif %}
           , 0)::double precision                     as withdrawable_cash,
       greatest(
           coalesce(portfolio_stats.cash_value, 0) + coalesce(pending_cash, 0) - coalesce(pending_order_stats.amount_sum, 0)
           , 0)::double precision                     as buying_power,
       greatest(trading_accounts.updated_at,
           deposit_stats.updated_at,
           account_stats.updated_at,
           portfolio_stats.updated_at,
           pending_order_stats.updated_at)::timestamp as updated_at
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
         left join deposit_stats using (trading_account_id)
         left join account_stats using (trading_account_id)
         left join portfolio_stats using (trading_account_id)
         left join (
                       select trading_account_id,
                              sum(amount_sum) as amount_sum,
                              sum(abs_amount_sum) as abs_amount_sum,
                              sum(cnt) as cnt,
                              max(updated_at) as updated_at
                       from (
                                select trading_account_id,
                                       sum(target_amount_delta - coalesce(executed_amount, 0))
                                       filter ( where target_amount_delta > 0 )                     as amount_sum,
                                       sum(abs(target_amount_delta - coalesce(executed_amount, 0))) as abs_amount_sum,
                                       count(*)                                                     as cnt,
                                       max(updated_at)                                              as updated_at
                                from {{ source('app', 'trading_collection_versions') }}
                                where status in ('PENDING_EXECUTION', 'PENDING')
                                  and source = 'MANUAL'
                                group by trading_account_id

                                union all

                                select trading_account_id,
                                       sum(target_amount_delta - coalesce(executed_amount, 0))
                                       filter ( where target_amount_delta > 0 )                     as amount_sum,
                                       sum(abs(target_amount_delta - coalesce(executed_amount, 0))) as abs_amount_sum,
                                       count(*)                                                     as cnt,
                                       max(updated_at)                                              as updated_at
                                from {{ source('app', 'trading_orders') }}
                                where status in ('PENDING_EXECUTION', 'PENDING')
                                  and source = 'MANUAL'
                                group by trading_account_id
                            ) t
                       group by trading_account_id
                   ) pending_order_stats using (trading_account_id)
