{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}

select portfolio_securities.id,
       portfolio_securities.name,
       base_tickers.symbol                as ticker_symbol,
       portfolio_securities.ticker_symbol as original_ticker_symbol,
       case
           when base_tickers.type = 'crypto'
               then base_tickers.type
           else portfolio_securities.type
           end                            as type
from {{ source('app', 'portfolio_securities') }}
         left join {{ ref('base_tickers') }}
                   on base_tickers.symbol in (portfolio_securities.ticker_symbol,
                                              regexp_replace(portfolio_securities.ticker_symbol, '\d{6}[CP]\d{8}$', ''),
                                              regexp_replace(portfolio_securities.ticker_symbol, '^CUR:(\w+)$', '\1.CC')
                       )
where base_tickers.symbol is not null
{% if var('portfolio_usd_enabled') %}
   or (portfolio_securities.type = 'cash' and portfolio_securities.ticker_symbol = 'CUR:USD')
{% endif %}
{% if var('portfolio_crypto_enabled') %}
   or base_tickers.type = 'crypto'
{% endif %}
