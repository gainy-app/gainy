select profile_id,
       profiles.created_at as sign_up_at,
       email,
       first_name,
       last_name,
       transaction_date,
       transaction_amount,
       transaction_status,
       case
           when transaction_source = 'AUTOMATIC' and transaction_amount is null
               then 'rebalance'
           when transaction_source = 'AUTOMATIC' and transaction_amount is not null
               then 'auto'
           when transaction_amount > 0
               then 'buy'
           when transaction_amount < 0
               then 'sell'
           end             as transaction_type,
       product_type,
       collection_id,
       collection_name,
       symbol,
       ticker_name,
       uniq_id,
       transactions.updated_at
from (
         select 'tcv_' || tcv.id    as uniq_id,
                profile_id,
                created_at          as transaction_date,
                target_amount_delta as transaction_amount,
                status              as transaction_status,
                source              as transaction_source,
                'ttf'               as product_type,
                collection_id,
                collections.name    as collection_name,
                null::text          as symbol,
                tcv.updated_at
         from {{ source('app', 'trading_collection_versions')}} tcv
                  left join {{ ref('collections')}} on collections.id = trading_collection_versions.collection_id

         union all

         select 'to_' || id         as uniq_id,
                profile_id,
                created_at          as transaction_date,
                target_amount_delta as transaction_amount,
                status              as transaction_status,
                source              as transaction_source,
                'ticker'            as product_type,
                null                as collection_id,
                null                as collection_name,
                symbol,
                base_tickers.name   as ticker_name,
                _to.updated_at
         from {{ source('app', 'trading_orders')}} _to
                  left join {{ ref('base_tickers')}} using (symbol)
     ) transactions
         left join (
                       select id as profile_id, email, first_name, last_name, created_at
                       from {{ source('app', 'profiles')}}
                   ) profiles using (profile_id)
