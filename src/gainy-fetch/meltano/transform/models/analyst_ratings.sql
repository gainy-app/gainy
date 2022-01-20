{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    post_hook=[
      index(this, 'symbol', true),
    ]
  )
}}

select distinct code::text                       as symbol,
       (analystratings ->> 'Buy')::int           as buy,
       (analystratings ->> 'Hold')::int          as hold,
       (analystratings ->> 'Sell')::int          as sell,
       (analystratings ->> 'Rating')::float      as rating,
       (analystratings ->> 'StrongBuy')::int     as strong_buy,
       (analystratings ->> 'StrongSell')::int    as strong_sell,
       (analystratings ->> 'TargetPrice')::float as target_price

from {{ source('eod', 'eod_fundamentals') }} f
inner join {{  ref('tickers') }} as t on f.code = t.symbol
