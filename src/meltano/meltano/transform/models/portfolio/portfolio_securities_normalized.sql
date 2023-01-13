{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('id'),
      fk('id', 'app', 'portfolio_securities', 'id'),
      index('original_ticker_symbol'),
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
                    when portfolio_securities.type = 'derivative' or ticker_options.contract_name is not null
                        then 'derivative'
                    when base_tickers.type = 'common stock'
                        then 'equity'
                    else portfolio_securities.type
                    end                                      as type,
               greatest(portfolio_securities.updated_at,
                        base_tickers.updated_at)             as updated_at
         from {{ source('app', 'portfolio_securities') }}
                  left join {{ ref('ticker_options') }} 
                            on ticker_options.contract_name = portfolio_securities.ticker_symbol
                  left join {{ ref('base_tickers') }}
                            -- crypto
                            on (portfolio_securities.type in ('crypto', 'cryptocurrency') and
                                base_tickers.symbol =
                                case
                                    when portfolio_securities.ticker_symbol ~ '^CUR:([^.]+).*$'
                                        then regexp_replace(portfolio_securities.ticker_symbol, '^CUR:([^.]+).*$',
                                                            '\1.CC')
                                    else portfolio_securities.ticker_symbol || '.CC'
                                    end)
                                -- options
                                or ((portfolio_securities.type = 'derivative' or ticker_options.contract_name is not null) and
                                    base_tickers.symbol =
                                    case
                                        when ticker_options.contract_name is not null
                                            then ticker_options.symbol
                                        when portfolio_securities.ticker_symbol ~ '\d{6}[CP]\d{8}$'
                                            then regexp_replace(portfolio_securities.ticker_symbol, '\d{6}[CP]\d{8}$',
                                                                '')
                                        else portfolio_securities.ticker_symbol
                                        end)
--                                 -- stocks
                                or (portfolio_securities.type not in ('crypto', 'cryptocurrency', 'derivative') and
                                    base_tickers.symbol = portfolio_securities.ticker_symbol)
         where (portfolio_securities.ticker_symbol not like 'CUR:%' or portfolio_securities.type in ('crypto', 'cryptocurrency', 'cash'))
         {% if not var('portfolio_crypto_enabled') %}
            and not (base_tickers.type = 'crypto')
         {% endif %}
         {% if not var('portfolio_usd_enabled') %}
            and not (portfolio_securities.type = 'cash' and portfolio_securities.ticker_symbol = 'CUR:USD')
         {% endif %}
     ) t

{% if is_incremental() %}
         left join {{ this }} old_portfolio_securities_normalized
                   using (id, name, ticker_symbol, original_ticker_symbol, type)
{% endif %}

where t.type is not null

{% if is_incremental() %}
  and old_portfolio_securities_normalized.id is null
{% endif %}
