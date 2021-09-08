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

with settings as
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
                    hp.close,
                    ROW_NUMBER() OVER (PARTITION BY hp.code, date_part('year', hp.date::timestamp), div(
                                date_part('month', hp.date::timestamp)::integer - 1, settings.month_period_divider)
                        ORDER BY hp.date)                                                                   AS idx
             from historical_prices hp
                      join settings ON true
         ),
     historical_prices_max_idx AS
         (
             select code, year, month, MAX(idx) as max_idx
             from historical_prices_summary hps
             group by code, year, month
         ),
     returns AS
         (
             SELECT hps.code,
                    hps_close.date,
                    hps.open,
                    hps_close.close,
                    CASE WHEN hps.open > 0 THEN (hps_close.close - hps.open) / hps.open END as return
             FROM historical_prices_summary hps
                      JOIN historical_prices_max_idx hpmi
                           ON hpmi.code = hps.code AND hpmi.year = hps.year AND hpmi.month = hps.month
                      JOIN historical_prices_summary hps_close
                           ON hpmi.code = hps_close.code AND hpmi.year = hps_close.year AND
                              hpmi.month = hps_close.month AND
                              hps_close.idx = hpmi.max_idx
             WHERE hps.idx = 1
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
from fundamentals f
         JOIN {{ ref('tickers') }} t ON t.symbol = f.code
         JOIN {{ ref('ticker_momentum_metrics') }} tmm ON tmm.symbol = f.code
         JOIN {{ ref('ticker_value_growth_metrics') }} tvgm ON tvgm.symbol = f.code
         LEFT JOIN downside_deviation ON downside_deviation.code = f.code
