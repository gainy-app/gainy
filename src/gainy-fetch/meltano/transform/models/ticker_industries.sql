{{
  config(
    materialized = "table",
    sort = "created_at",
    dist = "symbol",
    post_hook=[
      index(this, 'symbol', true),
      fk(this, 'industry_id', 'gainy_industries', 'id')
    ]
  )
}}

SELECT code                as symbol,
       gainy_industries.id as industry_id
FROM raw_ticker_industries
         JOIN {{ ref('tickers') }} ON tickers.symbol = raw_ticker_industries.code
         JOIN gainy_industries ON gainy_industries.name = "industry name"