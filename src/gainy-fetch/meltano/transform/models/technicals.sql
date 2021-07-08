{{
  config(
    materialized = "table",
    dist = "symbol",
    post_hook=[
      index(this, 'symbol', true),
      fk(this, 'symbol', 'tickers', 'symbol')
    ]
  )
}}

select code                                            as symbol,
       (technicals ->> 'Beta')::float                  as beta,
       (technicals ->> '50DayMA')::float               as day_50_ma,
       (technicals ->> '200DayMA')::float              as day_200_ma,
       (technicals ->> '52WeekLow')::float             as week_52_Low,
       (technicals ->> '52WeekHigh')::float            as week_52_High,
       (technicals ->> 'ShortRatio')::float            as short_ratio,
       (technicals ->> 'SharesShort')::float           as shares_short,
       (technicals ->> 'ShortPercent')::float          as short_percent,
       (technicals ->> 'SharesShortPriorMonth')::float as shares_short_prior_month

from fundamentals f inner join {{  ref('tickers') }} as t on f.code = t.symbol


