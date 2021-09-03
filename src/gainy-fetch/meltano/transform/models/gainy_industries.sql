{{
  config(
    materialized = "table",
    sort = "created_at",
    dist = "symbol",
    post_hook=[
      index(this, 'name', true),
    ]
  )
}}

SELECT DISTINCT "industry name" as name
FROM raw_ticker_industries