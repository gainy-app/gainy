{{
  config(
    materialized = "table",
    post_hook=[
      fk(this, 'symbol', 'tickers', 'symbol'),
      fk(this, 'industry_id', 'gainy_industries', 'id'),
      'create unique index if not exists {{ get_index_name(this, "industry_id__symbol") }} (industry_id, symbol)',
    ]
  )
}}

SELECT code                as symbol,
       gainy_industries.id as industry_id
FROM raw_ticker_industries
         JOIN {{ ref('tickers') }} ON tickers.symbol = raw_ticker_industries.code
         JOIN gainy_industries ON gainy_industries.name = "industry name"