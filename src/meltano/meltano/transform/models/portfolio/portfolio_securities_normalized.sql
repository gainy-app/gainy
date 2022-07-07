{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('id'),
      fk(this, 'id', 'app', 'portfolio_securities', 'id'),
      index(this, 'original_ticker_symbol'),
    ]
  )
}}

select t.*
from (
         select portfolio_securities.id,
                portfolio_securities.name,
                coalesce(base_tickers.symbol,
                         portfolio_securities.ticker_symbol) as ticker_symbol,
                case
                    when base_tickers.type = 'crypto'
                        then base_tickers.symbol
                    else portfolio_securities.ticker_symbol
                    end                                      as original_ticker_symbol,
                case
                    when base_tickers.type = 'crypto'
                        then 'crypto'
                    when portfolio_securities.type = 'derivative'
                        then 'derivative'
                    when base_tickers.type = 'common stock'
                        then 'equity'
                    else portfolio_securities.type
                    end                                      as type
         from {{ source('app', 'portfolio_securities') }}
                  left join {{ ref('base_tickers') }}
                            on base_tickers.symbol in (portfolio_securities.ticker_symbol,
                                                       regexp_replace(portfolio_securities.ticker_symbol, '\d{6}[CP]\d{8}$', ''))
                                 or (base_tickers.symbol = regexp_replace(portfolio_securities.ticker_symbol, '^CUR:([^.]+).*$', '\1.CC')
                                     and portfolio_securities.type != 'cash')
         where
             (
                 base_tickers.symbol is not null
                 {% if not var('portfolio_crypto_enabled') %}
                     and base_tickers.type != 'crypto'
                 {% endif %}
             )
         {% if var('portfolio_usd_enabled') %}
            or (portfolio_securities.type = 'cash' and portfolio_securities.ticker_symbol = 'CUR:USD')
         {% endif %}
     ) t

{% if is_incremental() %}
         left join portfolio_securities_normalized old_portfolio_securities_normalized
                   using (id, name, ticker_symbol, original_ticker_symbol, type)
where old_portfolio_securities_normalized is null
{% endif %}
