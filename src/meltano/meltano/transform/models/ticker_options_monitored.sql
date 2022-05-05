{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}

select contract_name
from {{ source('app', 'profile_holdings')}}
         join {{ source('app', 'portfolio_securities')}} on profile_holdings.security_id = portfolio_securities.id
         join {{ ref('ticker_options') }} on ticker_options.contract_name = portfolio_securities.ticker_symbol
where portfolio_securities.type = 'derivative' and profile_holdings.quantity > 0
group by contract_name
