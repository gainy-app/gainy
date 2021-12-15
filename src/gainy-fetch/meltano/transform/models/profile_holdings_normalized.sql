{{
  config(
    materialized = "view"
  )
}}

select profile_holdings.id as holding_id,
       profile_holdings.security_id,
       profile_holdings.profile_id,
       profile_holdings.account_id,
       profile_holdings.quantity,
       tickers.name,
       portfolio_securities_normalized.ticker_symbol,
       portfolio_securities_normalized.type
from {{ source('app', 'profile_holdings') }}
         join {{ ref('portfolio_securities_normalized') }}
              on portfolio_securities_normalized.id = profile_holdings.security_id
         join {{ ref('tickers') }}
              on tickers.symbol = portfolio_securities_normalized.ticker_symbol
where portfolio_securities_normalized.type in ('mutual fund', 'equity', 'etf', 'derivative', 'cash')
