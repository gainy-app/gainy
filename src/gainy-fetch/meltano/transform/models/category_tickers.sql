{{
  config(
    materialized = "table",
    sort = "created_at",
    dist = ['category_id', 'symbol'],
    indexes = [
      {'columns': ['category_id', 'symbol'], 'unique': True}
    ],
    post_hook=[
      fk(this, 'symbol', 'tickers', 'symbol')
    ]
  )
}}

/* TODO Defensive, Speculation, Momentum, Value, Growth */
(
    select c.id as category_id,
           t.symbol
    from tickers t
             join app.categories c ON c.name = 'ETF'
    WHERE t.type = 'ETF'
)
UNION
(
    select c.id as category_id,
           t.symbol
    from {{ ref('tickers') }} t
             join app.categories c ON c.name = 'Penny'
             LEFT JOIN historical_prices hp on t.symbol = hp.code
             LEFT JOIN historical_prices hp_next on t.symbol = hp_next.code AND hp_next.date::timestamp > hp.date::timestamp
    WHERE hp_next.code is null
      AND hp.close < 1
)
UNION
(
    WITH max_dividend_date as (SELECT code, MAX(date::timestamp) as date from dividends GROUP BY code),
         last_five_years_dividends as (
             SELECT d.code, COUNT(DISTINCT date_part('year', d.date::timestamp)) as cnt
             from dividends d
                      join max_dividend_date mdd ON mdd.code = d.code
             WHERE d.date::timestamp > NOW() - interval '5 years'
               AND date_part('year', d.date::timestamp) > date_part('year', mdd.date) - 5
             GROUP BY d.code
         ),
         last_sixty_months_dividends as (
             SELECT d.code, SUM(d.value) / 5 as avg_value_per_year
             from dividends d
             WHERE d.date::timestamp > NOW() - interval '5 years'
             GROUP BY d.code
         ),
         last_sixty_months_earnings as (
             SELECT eh.symbol, SUM(eh.eps_actual) / 5 as avg_value_per_year
             from {{ ref('earnings_history') }} eh
             WHERE eh.date::timestamp > NOW() - interval '5 years'
             GROUP BY eh.symbol
         )
    select c.id     as category_id,
           t.symbol as ticker_symbol
    from tickers t
             join app.categories c ON c.name = 'Dividend'
             join last_five_years_dividends lfyd ON lfyd.code = t.symbol
             join last_sixty_months_dividends lsmd ON lsmd.code = t.symbol
             join last_sixty_months_earnings lsme ON lsme.symbol = t.symbol
             join {{ ref('ticker_financials') }} tf on t.symbol = tf.symbol
             join {{ ref('highlights') }} h on t.symbol = h.symbol
    WHERE lfyd.cnt = 5
      AND tf.market_capitalization > 500000000
      AND h.dividend_share >= lsmd.avg_value_per_year
      AND lsme.avg_value_per_year / lsmd.avg_value_per_year > 1.67
)
