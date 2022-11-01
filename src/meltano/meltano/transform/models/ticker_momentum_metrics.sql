{{
  config(
    materialized = "table",
    post_hook=[
      pk('symbol'),
      fk('symbol', this.schema, 'tickers', 'symbol')
    ]
  )
}}

with settings (local_risk_free_rate) as (values (0.001)),
     momentum as
         (
             SELECT symbol,
                    gic_sector,
                    case when hpm.price_2m > 0 THEN hpm.price_1m / hpm.price_2m - 1 - settings.local_risk_free_rate END   AS MOM2,
                    case when hpm.price_13m > 0 THEN hpm.price_1m / hpm.price_13m - 1 - settings.local_risk_free_rate END AS MOM12
             from {{ ref('tickers') }}
                      join settings ON true
                      join {{ ref('historical_prices_marked') }} hpm using (symbol)
         ),
     momentum_risk_adj as
         (
             SELECT symbol,
                    m.gic_sector,
                    CASE WHEN ABS(ticker_metrics.stddev_3_years) > 0 THEN m.MOM2 / ticker_metrics.stddev_3_years END  as Risk_Adj_MOM2,
                    CASE WHEN ABS(ticker_metrics.stddev_3_years) > 0 THEN m.MOM12 / ticker_metrics.stddev_3_years END as Risk_Adj_MOM12
             from momentum m
                      JOIN {{ ref('ticker_metrics') }} using (symbol)
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
             SELECT mra.symbol,
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
             SELECT symbol,
                    GREATEST(-3, LEAST(3, Z_Score_MOM2))  as Windsored_Z_Score_MOM2,
                    GREATEST(-3, LEAST(3, Z_Score_MOM12)) as Windsored_Z_Score_MOM12
             from z_score zs
         )
SELECT symbol,
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
FROM {{ ref('tickers') }} t
    join momentum m using (symbol)
    join momentum_risk_adj mra using (symbol)
    join windsored_z_score wzs using (symbol)
    join z_score using (symbol)
    join momentum_risk_adj_stats ON momentum_risk_adj_stats.gic_sector = t.gic_sector
