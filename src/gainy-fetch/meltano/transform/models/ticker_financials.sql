{{
  config(
    materialized = "table",
    sort = "created_at",
    dist = "symbol",
    post_hook=[
      index(this, 'symbol', true),
      fk(this, 'symbol', 'tickers', 'symbol')
    ]
  )
}}

select h.symbol,
       pe_ratio,
       market_capitalization,
       highlight,
       random()::real                                                                      as price_change_today, /* TODO implement */
       (1000 * random())::real                                                             as current_price, /* TODO implement */
       CASE
           WHEN cf1.dividends_paid = 0 THEN NULL
           ELSE ((cf0.dividends_paid - cf1.dividends_paid) / cf1.dividends_paid)::real END as divident_growth,
       now()                                                                               as created_at
from {{ ref('highlights') }} h
         LEFT JOIN {{ ref ('ticker_highlights') }} th
on h.symbol = th.symbol
    LEFT JOIN {{ ref ('cash_flow') }} cf0 on h.symbol = cf0.symbol
    LEFT JOIN {{ ref ('cash_flow') }} cf0_next on cf0.symbol = cf0_next.symbol AND cf0_next.date > cf0.date
    LEFT JOIN {{ ref ('cash_flow') }} cf1 on h.symbol = cf1.symbol AND cf1.date < cf0.date AND ABS(cf1.dividends_paid) != ABS(cf0.dividends_paid)
    LEFT JOIN {{ ref ('cash_flow') }} cf1_next on cf1.symbol = cf1_next.symbol AND cf1_next.date > cf1.date AND cf1_next.date != cf0.date AND ABS(cf1_next.dividends_paid) != ABS(cf0.dividends_paid)
WHERE cf0_next.symbol is null AND cf1_next.symbol is null
