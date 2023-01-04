{{
  config(
    materialized = "incremental",
    unique_key = "contract_name",
    post_hook=[
      pk('contract_name'),
      'create unique index if not exists "contract_name__symbol" ON {{ this }} (contract_name, symbol)',
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}

select t.*,
       now() as updated_at
from (
         select distinct ticker_options.contract_name, ticker_options.symbol
         from {{ source('app', 'profile_holdings')}}
                  join {{ source('app', 'portfolio_securities')}} on profile_holdings.security_id = portfolio_securities.id
                  join {{ ref('ticker_options') }} on ticker_options.contract_name = portfolio_securities.ticker_symbol
         where profile_holdings.quantity > 0
     ) t
