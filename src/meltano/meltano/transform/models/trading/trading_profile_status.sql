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
                    sum(pending_orders_count)  as pending_orders_count,
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
       case
           when kyc_status.status is not null and kyc_status.status = 'APPROVED'
            then kyc_status.created_at
           end                                                          as kyc_passed_at,
       kyc_status.status                                                as kyc_status,
       kyc_status.message                                               as kyc_message,
       kyc_status.error_messages                                        as kyc_error_messages,
       kyc_status.error_codes                                           as kyc_error_codes,
       trading_funding_accounts.profile_id is not null                  as funding_account_connected,
       coalesce(deposited_funds, false)                                 as deposited_funds,
       coalesce(pending_cash, 0)::double precision                      as pending_cash,
       pending_orders_amount,
       pending_orders_count::int,
       pending_orders_sum,
       coalesce(withdrawable_cash, 0)::double precision                 as withdrawable_cash,
       coalesce(buying_power, 0)::double precision -
       coalesce(pending_fees, 0)::double precision                      as buying_power_minus_pending_fees,
       greatest(0,
                coalesce(buying_power, 0)::double precision -
                coalesce(pending_fees, 0)::double precision)            as buying_power,
       coalesce(pending_fees, 0)::double precision                      as pending_fees,
       greatest(kyc_status.created_at,
           trading_funding_accounts.updated_at,
           account_stats.updated_at)::timestamp                         as updated_at
from (
         select id as profile_id
         from {{ source('app', 'profiles') }}
     ) profiles
         left join (
                       with distinct_statuses as
                                (
                                    select distinct on (profile_id) *
                                    from {{ source('app', 'kyc_statuses') }}
                                    order by profile_id, created_at desc
                                ),
                            error_codes as (
                                               select id, string_agg(error_code, ',') as error_codes
                                               from (
                                                        select id, json_array_elements_text(error_codes) as error_code
                                                        from distinct_statuses
                                                    ) t
                                               group by id
                            )
                       select profile_id, status, message, error_messages, created_at, error_codes.error_codes
                       from distinct_statuses
                                join error_codes using (id)
                   ) kyc_status using (profile_id)
         left join (
                       select profile_id, max(updated_at) as updated_at
                       from {{ source('app', 'trading_funding_accounts') }}
                       group by profile_id
                   ) trading_funding_accounts using (profile_id)
         left join (
                       select profile_id, sum(amount) as pending_fees
                       from {{ source('app', 'invoices') }}
                                left join (
                                              select distinct invoice_id
                                              from {{ source('app', 'payment_transactions') }}
                                              where payment_transactions.status = 'PENDING'
                                          ) payment_transactions
                                          on payment_transactions.invoice_id = invoices.id
                       where status = 'PENDING'
                         and payment_transactions.invoice_id is null -- no already withdrawn tx
                       group by profile_id
                   ) invoices using (profile_id)
         left join account_stats using (profile_id)
