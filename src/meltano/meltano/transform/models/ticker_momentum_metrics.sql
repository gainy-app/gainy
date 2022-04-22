{{
  config(
    materialized = "table",
    post_hook=[
      index(this, 'symbol', true),
      fk(this, 'symbol', this.schema, 'tickers', 'symbol')
    ]
  )
}}

with tickers as (select * from {{ ref('tickers') }} where type != 'crypto'),
     settings (local_risk_free_rate) as (values (0.001)),
     weekly_prices as
         (
             SELECT symbol, datetime, adjusted_close
             from {{ ref('historical_prices_aggregated') }}
             where datetime > NOW() - interval '3 years'
               and period = '1w'
         ),
     weekly_prices2 as
         (
             select *,
                    first_value(adjusted_close) over (partition by symbol order by datetime rows between 1 preceding and 1 preceding) as prev_week_adjusted_close
             from weekly_prices
         ),
     stddev_3_years as
         (
             SELECT symbol                                                 as code,
                    stddev_pop(adjusted_close / prev_week_adjusted_close - 1) * pow(52, 0.5) as value
             from weekly_prices2
             where prev_week_adjusted_close > 0
             group by code
         ),
     momentum as
         (
             SELECT f.code,
                    t.gic_sector,
                    case when hpm.price_2m > 0 THEN hpm.price_1m / hpm.price_2m - 1 - settings.local_risk_free_rate END   AS MOM2,
                    case when hpm.price_13m > 0 THEN hpm.price_1m / hpm.price_13m - 1 - settings.local_risk_free_rate END AS MOM12
             from {{ source('eod', 'eod_fundamentals') }} f
                      join settings ON true
                      join tickers as t on f.code = t.symbol
                      join {{ ref('historical_prices_marked') }} hpm using (symbol)
         ),
     momentum_risk_adj as
         (
             SELECT m.code,
                    m.gic_sector,
                    CASE WHEN ABS(s3y.value) > 0 THEN m.MOM2 / s3y.value END  as Risk_Adj_MOM2,
                    CASE WHEN ABS(s3y.value) > 0 THEN m.MOM12 / s3y.value END as Risk_Adj_MOM12
             from momentum m
                      JOIN stddev_3_years s3y ON s3y.code = m.code
         ),
     momentum_risk_adj_stats as
         (
             SELECT gic_sector,
                    AVG(Risk_Adj_MOM2)     as AVG_Risk_Adj_MOM2,
                    stddev(Risk_Adj_MOM2)  as StdDev_Risk_Adj_MOM2,
                    AVG(Risk_Adj_MOM12)    as AVG_Risk_Adj_MOM12,
                    stddev(Risk_Adj_MOM12) as StdDev_Risk_Adj_MOM12
             from momentum_risk_adj mra
             GROUP BY mra.gic_sector
         ),
     z_score as
         (
             SELECT mra.code,
                    case when abs(mras.StdDev_Risk_Adj_MOM2) > 0
                             then (Risk_Adj_MOM2 - mras.AVG_Risk_Adj_MOM2) / mras.StdDev_Risk_Adj_MOM2
                        END as Z_Score_MOM2,
                    case when abs(mras.StdDev_Risk_Adj_MOM12) > 0
                             then (Risk_Adj_MOM12 - mras.AVG_Risk_Adj_MOM12) / mras.StdDev_Risk_Adj_MOM12
                        END as Z_Score_MOM12
             from momentum_risk_adj mra
                      join momentum_risk_adj_stats mras ON mras.gic_sector = mra.gic_sector
         ),
     windsored_z_score as
         (
             SELECT zs.code,
                    GREATEST(-3, LEAST(3, Z_Score_MOM2))  as Windsored_Z_Score_MOM2,
                    GREATEST(-3, LEAST(3, Z_Score_MOM12)) as Windsored_Z_Score_MOM12
             from z_score zs
         )
SELECT m.code as symbol,
       MOM2,
       MOM12,
       Risk_Adj_MOM2,
       Risk_Adj_MOM12,
       AVG_Risk_Adj_MOM2,
       StdDev_Risk_Adj_MOM2,
       AVG_Risk_Adj_MOM12,
       StdDev_Risk_Adj_MOM12,
       Z_Score_MOM2,
       Z_Score_MOM12,
       Windsored_Z_Score_MOM2,
       Windsored_Z_Score_MOM12,
       (wzs.Windsored_Z_Score_MOM2 + wzs.Windsored_Z_Score_MOM12) / 2 as combined_momentum_score
FROM tickers t
         join momentum m on m.code = t.symbol
    join momentum_risk_adj mra on mra.code = t.symbol
    join windsored_z_score wzs on wzs.code = t.symbol
    join z_score ON z_score.code = wzs.code
    join momentum_risk_adj_stats ON momentum_risk_adj_stats.gic_sector = t.gic_sector
