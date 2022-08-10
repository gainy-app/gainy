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
             SELECT code,
                    CASE WHEN open > 0 THEN (close - open) / open END as return
             FROM (
                      select distinct on (
                          code,
                          date_year
                          ) code,
                            date_year,
                            first_value(open) over (partition by code, date_year order by date)                                as open,
                            last_value(close)
                            over (partition by code, date_year order by date rows between current row and unbounded following) as close
                      from {{ ref('historical_prices') }}
                      order by code, date_year, date
                  ) t
         ),
     downside_deviation AS
         (
             SELECT code,
                    SQRT(SUM(POW(return, 2)) / COUNT(return)) as value
             FROM returns
             GROUP BY code
             having COUNT(return) > 0
         )
select symbol,
       downside_deviation.value                        as downside_deviation,
       (technicals ->> 'Beta')::float                  as beta,
--        (technicals ->> '50DayMA')::float               as day_50_ma,
--        (technicals ->> '200DayMA')::float              as day_200_ma,
--        (technicals ->> '52WeekLow')::float             as week_52_Low,
--        (technicals ->> '52WeekHigh')::float            as week_52_High,
       (technicals ->> 'ShortRatio')::float            as short_ratio,
--        (technicals ->> 'SharesShort')::float           as shares_short,
       (technicals ->> 'ShortPercent')::float          as short_percent
--        (technicals ->> 'SharesShortPriorMonth')::float as shares_short_prior_month
from {{ source('eod', 'eod_fundamentals') }}
         JOIN {{ ref('tickers') }} ON tickers.symbol = eod_fundamentals.code
         LEFT JOIN downside_deviation using (code)
where tickers.type != 'crypto'
