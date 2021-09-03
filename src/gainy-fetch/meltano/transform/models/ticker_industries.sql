{{
  config(
    materialized = "table",
    sort = "created_at",
    dist = "symbol",
    post_hook=[
      index(this, 'symbol', true),
      fk(this, 'symbol', 'tickers', 'symbol'),
      fk(this, 'industry_name', 'gainy_industries', 'name')
    ]
  )
}}

SELECT code            as symbol,
       "industry name" as industry_name
FROM raw_ticker_industries
         JOIN {{ ref('tickers') }} ON tickers.symbol = raw_ticker_industries.code
         JOIN {{ ref('gainy_industries') }} ON gainy_industries.name = "industry name"