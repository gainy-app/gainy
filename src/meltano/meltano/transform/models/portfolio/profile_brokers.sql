{{
  config(
    materialized = "view",
  )
}}


select distinct profile_id, uniq_id as broker_uniq_id
from {{ source('app', 'profile_plaid_access_tokens') }}
         join {{ ref('portfolio_brokers') }} on portfolio_brokers.plaid_institution_id = profile_plaid_access_tokens.institution_id

union distinct

select distinct profile_id, broker_uniq_id
from {{ ref('profile_holdings_normalized') }}
where broker_uniq_id is not null
