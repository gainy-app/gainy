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

/* TODO remove price_change_today and current_price */
select h.symbol,
       pe_ratio,
       market_capitalization,
       highlight,
       h.revenue_ttm,
       CASE
           WHEN d0.value = 0 THEN NULL
           WHEN d1.value IS NOT NULL THEN pow(d0.value / d1.value, 1.0 / 5)::real - 1
           WHEN d2.value IS NOT NULL THEN (d0.value / d2.value)::real - 1
           END                   as dividend_growth,
       hp1.close                 as quarter_ago_close,
       hp0.close / hp1.close - 1 as quarter_price_performance,
       now()                     as created_at
from {{ ref('highlights') }} h
    LEFT JOIN {{ ref ('ticker_highlights') }} th on h.symbol = th.symbol

    LEFT JOIN dividends d0 on h.symbol = d0.code
    LEFT JOIN dividends d0_next on h.symbol = d0_next.code AND d0_next.date:: timestamp > d0.date:: timestamp
    LEFT JOIN dividends d1 on h.symbol = d1.code AND d1.date:: timestamp < d0.date:: timestamp - interval '5 years'
    LEFT JOIN dividends d1_next on h.symbol = d1_next.code AND d1_next.date:: timestamp < d0.date:: timestamp - interval '5 years' AND d1_next.date:: timestamp > d1.date:: timestamp
    LEFT JOIN dividends d2 on h.symbol = d2.code AND d2.date:: timestamp < d0.date:: timestamp - interval '1 years'
    LEFT JOIN dividends d2_next on h.symbol = d2_next.code AND d2_next.date:: timestamp < d0.date:: timestamp - interval '1 years' AND d2_next.date:: timestamp > d2.date:: timestamp

    LEFT JOIN historical_prices hp0 on h.symbol = hp0.code
    LEFT JOIN historical_prices hp0_next on h.symbol = hp0_next.code AND hp0_next.date::timestamp > hp0.date::timestamp
    LEFT JOIN historical_prices hp1 on h.symbol = hp1.code AND hp1.date::timestamp < NOW() - interval '3 months'
    LEFT JOIN historical_prices hp1_next on h.symbol = hp1_next.code AND hp1_next.date::timestamp < NOW() - interval '3 months' AND hp1_next.date::timestamp > hp1.date::timestamp

WHERE d0_next.code is null
  AND d1_next.code is null
  AND d2_next.code is null
  AND hp0_next.code is null
  AND hp1_next.code is null