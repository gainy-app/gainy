{{
  config(
    materialized = "table",
    post_hook=[
      fk(this, 'category_id', 'categories', 'id'),
      fk(this, 'symbol', 'tickers', 'symbol'),
      'create unique index if not exists {{ get_index_name(this, "symbol__category_id") }} (symbol, category_id)',
    ]
  )
}}

(
    WITH downside_deviation_stats AS
             (
                 SELECT tickers.gic_sector,
                        percentile_cont(0.5) WITHIN GROUP (ORDER BY downside_deviation) as median
                 FROM technicals
                          JOIN tickers ON tickers.symbol = technicals.symbol
                 GROUP BY tickers.gic_sector
             )
    select c.id as category_id,
           t.symbol
    from tickers t
             join {{ ref('categories') }} c ON c.name = 'Defensive'
             JOIN technicals t2 on t.symbol = t2.symbol
             JOIN downside_deviation_stats on downside_deviation_stats.gic_sector = t.gic_sector
    WHERE t2.beta < 1
      AND t2.downside_deviation < downside_deviation_stats.median
      AND t.gic_sector IN ('Consumer Staples', 'Utilities', 'Health Care', 'Communication Services')
)
UNION
(
    WITH max_eps AS
             (
                 select t.symbol,
                        MAX(eh.eps_actual) as value
                 from tickers t
                          JOIN earnings_history eh on t.symbol = eh.symbol
                 GROUP BY t.symbol
             ),
         options_stats as
             (
                 SELECT code,
                        SUM(callvolume) * 100 as call_option_shares_deliverable_outstanding
                 FROM {{ source('eod', 'eod_options') }} options
                 WHERE expirationdate::timestamp > NOW()
                 GROUP BY code
             )
    select c.id as category_id,
           t.symbol
    from tickers t
             join {{ ref('categories') }} c ON c.name = 'Speculation'
             JOIN {{ ref('technicals') }} t2 on t.symbol = t2.symbol
             JOIN {{ source('eod', 'eod_fundamentals') }} f ON f.code = t.symbol
             JOIN max_eps on max_eps.symbol = t.symbol
             JOIN options_stats on options_stats.code = t.symbol
    WHERE t2.beta > 3
       OR max_eps.value < 0
       OR max_eps.value IS NULL
       OR call_option_shares_deliverable_outstanding > (sharesstats ->> 'SharesOutstanding')::bigint
)
UNION
(
    select c.id as category_id,
           t.symbol
    from {{ ref('tickers') }} t
             join {{ ref('categories') }} c ON c.name = 'ETF'
    WHERE t.type = 'ETF'
)
UNION
(
    select c.id as category_id,
           t.symbol
    from {{ ref('tickers') }} t
             join {{ ref('categories') }} c ON c.name = 'Penny'
             LEFT JOIN {{ ref('historical_prices') }} hp on t.symbol = hp.code
             LEFT JOIN {{ ref('historical_prices') }} hp_next on t.symbol = hp_next.code AND hp_next.date::timestamp > hp.date::timestamp
    WHERE hp_next.code is null
      AND hp.close < 1
)
UNION
(
    WITH dividends as (select * from {{ source('eod', 'eod_dividends') }}),
         max_dividend_date as (SELECT code, MAX(date::timestamp) as date from dividends GROUP BY code),
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
             join {{ ref('categories') }} c ON c.name = 'Dividend'
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
UNION
(
    select c.id as category_id,
           t.symbol
    from {{ ref('tickers') }} t
             join {{ ref('categories') }} c ON c.name = 'Momentum'
             join {{ ref('technicals') }} ON technicals.symbol = t.symbol
    WHERE technicals.combined_momentum_score > 0
)
UNION
(
    select c.id as category_id,
           t.symbol
    from {{ ref('tickers') }} t
             join {{ ref('categories') }} c ON c.name = 'Value'
             join {{ ref('technicals') }} ON technicals.symbol = t.symbol
    WHERE technicals.value_score > 0
      AND technicals.growth_score < 0
)
UNION
(
    select c.id as category_id,
           t.symbol
    from {{ ref('tickers') }} t
             join {{ ref('categories') }} c ON c.name = 'Growth'
             join {{ ref('technicals') }} ON technicals.symbol = t.symbol
    WHERE technicals.value_score < 0
      AND technicals.growth_score > 0
)
