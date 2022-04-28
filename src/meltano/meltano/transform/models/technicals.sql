{{
  config(
    materialized = "table",
    dist = "symbol",
    post_hook=[
      pk('symbol'),
    ]
  )
}}

with tickers as (select * from {{ ref('tickers') }} where type != 'crypto'),
     ticker_momentum_metrics as (select * from {{ ref('ticker_momentum_metrics') }}),
     ticker_value_growth_metrics as (select * from {{ ref('ticker_value_growth_metrics') }}),
     settings as
         (
             select 12 as month_period_divider
         ),
     historical_prices_summary AS
         (
             select hp.code,
                    date_part('year', hp.date::timestamp)                                                   as year,
                    div(date_part('month', hp.date::timestamp)::integer - 1, settings.month_period_divider) as month,
                    hp.date,
                    hp.open,
                    hp.close
             from {{ ref('historical_prices') }} hp
                      join settings ON true
         ),
     period_open AS
         (
             select distinct on (code, year, month) *
             from historical_prices_summary
             order by code, year, month, date
         ),
     period_close AS
         (
             select distinct on (code, year, month) *
             from historical_prices_summary
             order by code desc, year desc, month desc, date desc
         ),
     returns AS
         (
             SELECT po.code,
                    pc.date,
                    po.open,
                    pc.close,
                    CASE WHEN po.open > 0 THEN (pc.close - po.open) / po.open END as return
             FROM period_open po
                      JOIN period_close pc ON pc.code = po.code AND pc.year = po.year AND pc.month = po.month
         ),
     downside_deviation AS
         (
             SELECT code,
                    SQRT(SUM(POW(return, 2)) / COUNT(return)) as value
             FROM returns
             GROUP BY code
             having COUNT(return) > 0
         )
select f.code                                          as symbol,
       tmm.combined_momentum_score,
       tvgm.growth_score,
       tvgm.value_score,
       downside_deviation.value                        as downside_deviation,
       (technicals ->> 'Beta')::float                  as beta,
       (technicals ->> '50DayMA')::float               as day_50_ma,
       (technicals ->> '200DayMA')::float              as day_200_ma,
       (technicals ->> '52WeekLow')::float             as week_52_Low,
       (technicals ->> '52WeekHigh')::float            as week_52_High,
       (technicals ->> 'ShortRatio')::float            as short_ratio,
       (technicals ->> 'SharesShort')::float           as shares_short,
       (technicals ->> 'ShortPercent')::float          as short_percent,
       (technicals ->> 'SharesShortPriorMonth')::float as shares_short_prior_month
from {{ source('eod', 'eod_fundamentals') }} f
         JOIN tickers t ON t.symbol = f.code
         LEFT JOIN ticker_momentum_metrics tmm ON tmm.symbol = f.code
         LEFT JOIN ticker_value_growth_metrics tvgm ON tvgm.symbol = f.code
         LEFT JOIN downside_deviation ON downside_deviation.code = f.code
