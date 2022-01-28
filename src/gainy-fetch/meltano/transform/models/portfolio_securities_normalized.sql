{{
  config(
    materialized = "view"
  )
}}

select portfolio_securities.id,
       portfolio_securities.name,
       case
           when portfolio_securities.type = 'derivative'
               then regexp_replace(portfolio_securities.ticker_symbol, '\d{6}[CP]\d{8}$', '')
           else portfolio_securities.ticker_symbol
           end                            as ticker_symbol,
       portfolio_securities.ticker_symbol as original_ticker_symbol,
       portfolio_securities.type
from {{ source('app', 'portfolio_securities') }}
where portfolio_securities.type in ('mutual fund', 'equity', 'etf', 'derivative', 'cash')
   or (portfolio_securities.type = 'cash' and portfolio_securities.ticker_symbol = 'CUR:USD')