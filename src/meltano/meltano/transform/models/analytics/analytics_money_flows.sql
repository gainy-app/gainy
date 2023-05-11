select profile_id,
       profiles.created_at           as sign_up_at,
       email,
       first_name,
       last_name,
       trading_money_flow.created_at as transaction_date,
       trading_money_flow.amount     as transaction_amount,
       trading_money_flow.status     as transaction_status,
       case
           when trading_money_flow.amount > 0
               then 'deposit'
           else 'withdraw'
           end                       as transaction_type,
       trading_money_flow.error_message,
       id,
       trading_money_flow.updated_at
from {{ source('app', 'trading_money_flow') }}
         left join (
                       select id as profile_id, email, first_name, last_name, created_at
                       from {{ source('app', 'profiles') }}
                   ) profiles using (profile_id)
