{{
  config(
    materialized = "view",
  )
}}


-- Deal execution
select profile_id,
       ('order_executed_' || uniq_id)                                              as uniq_id,
       null::timestamp                                                             as send_at,
       json_build_object('en', 'Trade confirmation')                               as title,
       json_build_object('en', case when amount > 0 then 'Buy' else 'Sell' end ||
                               ' $' || round(abs(amount), 2) || ' ' || asset_name) as text,
       json_build_object('t', 9, 'id', trading_history_uniq_id)                    as data,
       true                                                                        as is_test,
       true                                                                        as is_push,
       true                                                                        as is_shown_in_app,
       '094f1363-da90-4477-bc69-6c333d987a52'                                      as template_id
from (
         select trading_collection_versions.profile_id,
                'trading_collection_versions_' || trading_collection_versions.id as uniq_id,
                target_amount_delta                                              as amount,
                collections.name                                                 as asset_name,
                trading_history.uniq_id                                          as trading_history_uniq_id,
                executed_at
         from {{ source('app', 'trading_collection_versions') }}
                  join {{ ref('collections') }} on collections.id = trading_collection_versions.collection_id
                  left join {{ ref('trading_history') }}
                      on trading_history.trading_collection_version_id = trading_collection_versions.id
         where source = 'MANUAL'
           and status = 'EXECUTED_FULLY'

         union all

         select trading_orders.profile_id,
                'trading_orders_' || id as uniq_id,
                target_amount_delta     as amount,
                symbol                  as asset_name,
                trading_history.uniq_id as trading_history_uniq_id,
                executed_at
         from {{ source('app', 'trading_orders') }}
                  left join {{ ref('trading_history') }}
                      on trading_history.trading_order_id = trading_orders.id
         where source = 'MANUAL'
           and status = 'EXECUTED_FULLY'
     ) t
where executed_at between now() - interval '1 hours' and now()

union all

-- TTF rebalanced
select profile_id,
       ('ttf_rebalanced_' || profile_id || '_' || date_trunc('week', executed_at::date))       as uniq_id,
       null::timestamp as send_at,
       json_build_object('en', 'Portfolio rebalancing') as title,
       json_build_object('en', 'Your TTFs were automatically rebalanced. ' ||
                               'Now they match our optimized model portfolios. ' ||
                               'It is a standard procedure for Gainy’s portfolio management.') as text,
       json_build_object('t', 11)                                                              as data,
       true                                                                                    as is_test,
       true                                                                                    as is_push,
       true                                                                                    as is_shown_in_app,
       'beb30a52-a65b-496c-90bb-3d50c5e1aaf0'                                                  as template_id
from (
         select profile_id, max(executed_at) as executed_at
         from {{ source('app', 'trading_collection_versions') }}
         where source = 'AUTOMATIC' and status = 'EXECUTED_FULLY'
         group by profile_id
    ) t
where executed_at between now() - interval '1 hours' and now()

union all

-- KYC status changed
select profile_id,
       ('kyc_status_' || id)                  as uniq_id,
       null::timestamp                        as send_at,
       null::json                             as title,
       json_build_object(
               'en',
               case
                   when latest_kyc_status.status = 'APPROVED'
                       then 'Your trading account is open!'
                   when latest_kyc_status.status = 'INFO_REQUIRED'
                       then 'We need additional information to open your trading account.'
                   when latest_kyc_status.status = 'DOC_REQUIRED'
                       then 'Upload documents to open your trading account.'
                   end
           )                                  as text,
       json_build_object('t', 10,
           'status', latest_kyc_status.status
           )                                  as data,
       true                                   as is_test,
       true                                   as is_push,
       true                                   as is_shown_in_app,
       'a79620ce-03f0-4c54-84a7-e66934c1c0d6' as template_id
from (
         select distinct on (
             profile_id
             ) id,
               profile_id,
               status,
               created_at
         from {{ source('app', 'kyc_statuses') }}
         where status in ('APPROVED', 'INFO_REQUIRED', 'DOC_REQUIRED')
         order by profile_id, created_at desc
     ) latest_kyc_status
         left join (
                       select distinct on (
                           kyc_statuses.profile_id
                           ) kyc_statuses.profile_id,
                             kyc_statuses.status
                       from {{ source('app', 'kyc_statuses') }}
                                join {{ source('app', 'notifications') }}
                                     on notifications.profile_id = kyc_statuses.profile_id
                                         and notifications.uniq_id = 'kyc_status_' || kyc_statuses.id
                       where kyc_statuses.status in ('APPROVED', 'INFO_REQUIRED', 'DOC_REQUIRED')
                       order by kyc_statuses.profile_id, kyc_statuses.created_at desc
                   ) last_notified_status using (profile_id)
where (last_notified_status.profile_id is null -- no notifications
    or last_notified_status.status != latest_kyc_status.status)
  and latest_kyc_status.created_at between now() - interval '1 hours' and now()

union all

-- Successful money flows
select trading_money_flow.profile_id,
       ('money_flow_success_' || id)                            as uniq_id,
       null::timestamp                                          as send_at,
       null::json                                               as title,
       json_build_object(
               'en',
               case
                   when trading_money_flow.amount > 0
                       then 'Your account has been topped up!'
                   else 'Your withdraw request has been processed.'
                   end
           )                                                    as text,
       json_build_object('t', 9, 'id', trading_history.uniq_id) as data,
       true                                                     as is_test,
       true                                                     as is_push,
       true                                                     as is_shown_in_app,
       '8c9d99c1-0df2-4b12-9ba4-bf40b6871265'                   as template_id
from {{ source('app', 'trading_money_flow') }}
         left join {{ ref('trading_history') }}
             on trading_history.money_flow_id = trading_money_flow.id
where status = 'SUCCESS'
  and trading_money_flow.updated_at between now() - interval '1 hours' and now()
