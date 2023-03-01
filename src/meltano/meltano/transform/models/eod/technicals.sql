{{
  config(
    materialized = "table",
    dist = "symbol",
    post_hook=[
      pk('symbol'),
    ]
  )
}}


with returns AS
         (
             SELECT symbol,
                    CASE WHEN open > 0 THEN (close - open) / open END as return
             FROM (
                      select distinct on (
                          symbol,
                          date_year
                          ) symbol,
                            date_year,
                            first_value(open) over (partition by symbol, date_year order by date)                                as open,
                            last_value(close)
                            over (partition by symbol, date_year order by date rows between current row and unbounded following) as close
                      from {{ ref('historical_prices') }}
                      order by symbol, date_year, date
                  ) t
         ),
     downside_deviation AS
         (
             SELECT symbol as code,
                    SQRT(SUM(POW(return, 2)) / COUNT(return)) as value
             FROM returns
             GROUP BY symbol
             having COUNT(return) > 0
         )
select symbol,
       downside_deviation.value                        as downside_deviation,
       (technicals ->> 'Beta')::float                  as beta,
       (technicals ->> 'ShortRatio')::float            as short_ratio,
       (technicals ->> 'ShortPercent')::float          as short_percent
from {{ source('eod', 'eod_fundamentals') }}
         JOIN {{ ref('tickers') }} ON tickers.symbol = eod_fundamentals.code
         LEFT JOIN downside_deviation using (code)
where tickers.type != 'crypto'
