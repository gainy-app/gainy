{{
  config(
    materialized = "view",
  )
}}


with data as
         (
             select profile_id,
                    'tmf_' || id                                            as uniq_id,
                    case when amount > 0 then 'deposit' else 'withdraw' end as type,
                    null::int                                               as trading_collection_version_id,
                    null::int                                               as trading_order_id,
                    trading_money_flow.id                                   as money_flow_id,
                    null::int                                               as payment_transaction_id,
                    case when amount > 0 then 'Deposit' else 'Withdraw' end as name,
                    created_at                                              as datetime,
                    amount,
                    json_build_object('deposit', amount > 0,
                                      'withdraw', amount < 0,
                                      'pending', status in ('PENDING', 'APPROVED') or status is null,
                                      'error', coalesce(status, '') = 'FAILED'
                        )                                                   as tags
             from {{ source('app', 'trading_money_flow') }}

             union all

             select payment_transactions.profile_id,
                    'pt_' || payment_transactions.id as uniq_id,
                    'trading_fee'                    as type,
                    null::int                        as trading_collection_version_id,
                    null::int                        as trading_order_id,
                    null::int                        as money_flow_id,
                    payment_transactions.id          as payment_transaction_id,
                    'Service fee'                    as name,
                    payment_transactions.created_at  as datetime,
                    amount::double precision,
                    json_build_object('fee', true,
                                      'pending', payment_transactions.status = 'PENDING' or payment_transactions.status is null,
                                      'error', coalesce(payment_transactions.status, '') = 'FAILED'
                        )                            as tags
             from {{ source('app', 'payment_transactions') }}
                      join {{ source('app', 'invoices') }} on invoices.id = payment_transactions.invoice_id

             union all

             select profile_id,
                    'tcv_' || trading_collection_versions.id        as uniq_id,
                    'ttf_transaction'                               as type,
                    trading_collection_versions.id                  as trading_collection_version_id,
                    null::int                                       as trading_order_id,
                    null::int                                       as money_flow_id,
                    null::int                                       as payment_transaction_id,
                    collections.name,
                    created_at                                      as datetime,
                    trading_collection_versions.target_amount_delta as amount,
                    json_build_object('ttf', true,
                                      'buy', target_amount_delta > 0,
                                      'sell', target_amount_delta < 0,
                                      'is_manual', source = 'MANUAL',
                                      'pending', status in ('PENDING', 'PENDING_EXECUTION') or status is null,
                                      'cancelled', coalesce(status, '') = 'CANCELLED',
                                      'error', coalesce(status, '') = 'FAILED'
                        )                                           as tags
             from {{ source('app', 'trading_collection_versions') }}
                      join {{ ref('collections') }} on collections.id = trading_collection_versions.collection_id

             union all

             select profile_id,
                    'to_' || trading_orders.id         as uniq_id,
                    'ticker_transaction'               as type,
                    null::int                          as trading_collection_version_id,
                    trading_orders.id                  as trading_order_id,
                    null::int                          as money_flow_id,
                    null::int                          as payment_transaction_id,
                    base_tickers.name,
                    created_at                         as datetime,
                    trading_orders.target_amount_delta as amount,
                    json_build_object('ticker', true,
                                      'buy', target_amount_delta > 0,
                                      'sell', target_amount_delta < 0,
                                      'is_manual', source = 'MANUAL',
                                      'pending', status in ('PENDING', 'PENDING_EXECUTION') or status is null,
                                      'cancelled', coalesce(status, '') = 'CANCELLED',
                                      'error', coalesce(status, '') = 'FAILED'
                        )                              as tags
             from {{ source('app', 'trading_orders') }}
                      left join {{ ref('base_tickers') }} on base_tickers.symbol = trading_orders.symbol
         )
select data.profile_id,
       data.uniq_id,
       data.trading_collection_version_id,
       data.trading_order_id,
       data.money_flow_id,
       data.payment_transaction_id,
       data.type,
       data.name,
       data.datetime,
       data.amount::double precision,
       data.tags
from data
where data.profile_id is not null
