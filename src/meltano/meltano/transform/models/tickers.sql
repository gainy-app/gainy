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

             union distinct

             select symbol
             from {{ source('polygon', 'polygon_stocks_historical_prices') }}
             where t >= extract(epoch from now() - interval '7 days') * 1000
             group by symbol
         ),
     latest_crypto_price as
         (
             select regexp_replace(symbol, 'USD$', '.CC') as symbol
             from {{ source('polygon', 'polygon_crypto_historical_prices') }}
             where t >= extract(epoch from now() - interval '7 days') * 1000
             group by symbol
         ),
     tradeable_tickers as
         (
             select distinct public.normalize_drivewealth_symbol(symbol) as symbol
             from {{ source('app', 'drivewealth_instruments') }}
             where status = 'ACTIVE'
         ),
     ms_enabled_tickers as
         (
             SELECT distinct symbol
             FROM {{ ref('base_tickers') }}
                      LEFT JOIN {{ ref('ticker_metrics') }} using (symbol)
             WHERE type IN ('common stock', 'preferred stock')
               AND market_capitalization >= 100000000
               AND avg_volume_90d_money > 500000

             union distinct

             SELECT distinct symbol
             FROM {{ ref('base_tickers') }}
                      LEFT JOIN {{ ref('ticker_metrics') }} using (symbol)
             WHERE type IN ('etf', 'fund', 'mutual fund')
               AND avg_volume_90d_money > 500000
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
       tradeable_tickers.symbol is not null  as is_trading_enabled,
       ms_enabled_tickers.symbol is not null as ms_enabled,
       now()::timestamp                      as updated_at
from {{ ref('base_tickers') }}
         join (select symbol from {{ ref('week_trading_sessions_static') }} group by symbol) t using (symbol)
         left join latest_price using (symbol)
         left join latest_crypto_price using (symbol)
         left join tradeable_tickers using (symbol)
         left join ms_enabled_tickers using (symbol)
where ((base_tickers.description is not null and length(base_tickers.description) >= 5) or type = 'index')
  and (latest_price.symbol is not null or latest_crypto_price.symbol is not null)
  and type in (
{% if var('crypto_enabled') %}
         'crypto',
{% endif %}
{% if var('index_enabled') %}
         'index',
{% endif %}
         'fund',
         'etf',
         'mutual fund',
         'preferred stock',
         'common stock'
        )
