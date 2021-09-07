{{
  config(
    materialized = "table",
    dist = "symbol",
    post_hook=[
      index(this, 'symbol', true),
      fk(this, 'symbol', 'tickers', 'symbol'),
    ]
  )
}}

select f.code                                          as symbol,
       tmm.combined_momentum_score,
       tvgm.growth_score,
       tvgm.value_score,
       (technicals ->> 'Beta')::float                  as beta,
       (technicals ->> '50DayMA')::float               as day_50_ma,
       (technicals ->> '200DayMA')::float              as day_200_ma,
       (technicals ->> '52WeekLow')::float             as week_52_Low,
       (technicals ->> '52WeekHigh')::float            as week_52_High,
       (technicals ->> 'ShortRatio')::float            as short_ratio,
       (technicals ->> 'SharesShort')::float           as shares_short,
       (technicals ->> 'ShortPercent')::float          as short_percent,
       (technicals ->> 'SharesShortPriorMonth')::float as shares_short_prior_month
from fundamentals f
         JOIN {{ ref('tickers') }} t ON t.symbol = f.code
         JOIN {{ ref('ticker_momentum_metrics') }} tmm ON tmm.symbol = f.code
         JOIN {{ ref('ticker_value_growth_metrics') }} tvgm ON tvgm.symbol = f.code
