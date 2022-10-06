{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    post_hook=[
      pk('symbol'),
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})::date',
    ]
  )
}}


with latest_price as
         (
             select code as symbol
             from {{ source('eod', 'eod_historical_prices') }}
             where date::date >= now() - interval '7 days'
             group by code
         ),
     latest_crypto_price as
         (
             select regexp_replace(symbol, 'USD$', '.CC') as symbol
             from {{ source('polygon', 'polygon_crypto_historical_prices') }}
             where t >= extract(epoch from now() - interval '7 days') * 1000
             group by symbol
         )
select base_tickers.symbol,
       base_tickers.type,
       base_tickers.name,
       base_tickers.description,
       base_tickers.phone,
       base_tickers.logo_url,
       base_tickers.web_url,
       base_tickers.ipo_date,
       base_tickers.sector,
       base_tickers.industry,
       base_tickers.gic_sector,
       base_tickers.gic_group,
       base_tickers.gic_industry,
       base_tickers.gic_sub_industry,
       base_tickers.exchange,
       base_tickers.exchange_canonical,
       base_tickers.country_name,
       now()::timestamp as updated_at
from {{ ref('base_tickers') }}
         join (select symbol from {{ ref('week_trading_sessions_static') }} group by symbol) t using (symbol)
         left join latest_price using (symbol)
         left join latest_crypto_price using (symbol)
where ((base_tickers.description is not null and length(base_tickers.description) >= 5) or type = 'index')
  and (latest_price.symbol is not null or latest_crypto_price.symbol is not null)
  and type in ('fund', 'etf', 'mutual fund', 'preferred stock', 'common stock', 'crypto', 'index')
