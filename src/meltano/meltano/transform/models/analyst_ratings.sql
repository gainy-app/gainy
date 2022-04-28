{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    post_hook=[
      index(this, 'symbol', true),
    ]
  )
}}

select distinct
    eod_fundamentals.code::text as symbol,
    (eod_fundamentals.analystratings ->> 'Buy')::int as buy,
    (eod_fundamentals.analystratings ->> 'Hold')::int as hold,
    (eod_fundamentals.analystratings ->> 'Sell')::int as sell,
    (eod_fundamentals.analystratings ->> 'Rating')::float as rating,
    (eod_fundamentals.analystratings ->> 'StrongBuy')::int as strong_buy,
    (eod_fundamentals.analystratings ->> 'StrongSell')::int as strong_sell,
    (eod_fundamentals.analystratings ->> 'TargetPrice')::float as target_price

from {{ source('eod', 'eod_fundamentals') }}
inner join {{ ref('tickers') }} on eod_fundamentals.code = tickers.symbol
