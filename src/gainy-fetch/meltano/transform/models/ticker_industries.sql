{{
  config(
    materialized = "table",
    indexes = [
      { 'columns': ['industry_id', 'symbol'], 'unique': true },
    ],
    post_hook=[
      fk(this, 'symbol', 'tickers', 'symbol'),
      fk(this, 'industry_id', 'gainy_industries', 'id'),
    ]
  )
}}

SELECT code                as symbol,
       gainy_industries.id as industry_id
FROM raw_ticker_industries
         JOIN {{ ref('tickers') }} ON tickers.symbol = raw_ticker_industries.code
         JOIN gainy_industries ON gainy_industries.name = "industry name"