{{
  config(
    materialized = "table",
    sort = "updated_at",
    dist = "symbol",
    post_hook=[
      index(this, 'symbol', true)
    ]
  )
}}

select symbol, 'IPOed last 3 months' as highlight
    from tickers
    where ipo_date > '2021-04-13'
