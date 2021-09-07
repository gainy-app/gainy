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

with settings (local_risk_free_rate) as (values (0.001)),
     weekly_prices as
         (
             SELECT hp.code, hp.date::timestamp as date, hp.close
             from historical_prices hp
                      JOIN {{ ref('tickers') }} t ON t.symbol = hp.code
                      left join historical_prices hp1 ON hp1.code = hp.code AND
                                                         To_char(hp1.date::timestamp, 'IYYY-IW') =
                                                         To_char(hp.date::timestamp, 'IYYY-IW') AND
                                                         hp1.date::timestamp > hp.date::timestamp
             where hp.date::timestamp > NOW() - interval '3 years'
               AND hp1.code IS NULL
         ),
     stddev_3_years as
         (
             SELECT wp.code, stddev_pop(wp.close / wp_prev.close - 1) * pow(52, 0.5) as value
             from weekly_prices wp
                      JOIN weekly_prices wp_prev
                           ON wp.code = wp_prev.code AND wp_prev.date = wp.date - interval '1 week'
             group by wp.code
         ),
     momentum as
         (
             SELECT f.code,
                    t.gic_sector,
                    hp0.close / hp1.close - 1 - settings.local_risk_free_rate AS MOM2,
                    hp0.close / hp2.close - 1 - settings.local_risk_free_rate AS MOM12
             from fundamentals f
                      join settings ON true
                      left join historical_prices hp0
                                on hp0.code = f.code AND hp0.date::timestamp < NOW() - interval '1 month'
                      left join historical_prices hp0_next
                                on hp0_next.code = f.code AND
                                   hp0_next.date::timestamp < NOW() - interval '1 month' AND
                                   hp0_next.date::timestamp > hp0.date::timestamp

                      left join historical_prices hp1
                                on hp1.code = f.code AND hp1.date::timestamp < NOW() - interval '2 month'
                      left join historical_prices hp1_next
                                on hp1_next.code = f.code AND
                                   hp1_next.date::timestamp < NOW() - interval '2 month' AND
                                   hp1_next.date::timestamp > hp1.date::timestamp

                      left join historical_prices hp2
                                on hp2.code = f.code AND hp2.date::timestamp < NOW() - interval '13 month'
                      left join historical_prices hp2_next
                                on hp2_next.code = f.code AND
                                   hp2_next.date::timestamp < NOW() - interval '13 month' AND
                                   hp2_next.date::timestamp > hp2.date::timestamp

                      inner join {{ ref('tickers') }} as t on f.code = t.symbol
             WHERE hp0_next.code IS NULL
               AND hp1_next.code IS NULL
               AND hp2_next.code IS NULL
         ),
     momentum_risk_adj as
         (
             SELECT m.code,
                    m.gic_sector,
                    m.MOM2 / s3y.value  as Risk_Adj_MOM2,
                    m.MOM12 / s3y.value as Risk_Adj_MOM12
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
                    (Risk_Adj_MOM2 - mras.AVG_Risk_Adj_MOM2) / mras.StdDev_Risk_Adj_MOM2    as Z_Score_MOM2,
                    (Risk_Adj_MOM12 - mras.AVG_Risk_Adj_MOM12) / mras.StdDev_Risk_Adj_MOM12 as Z_Score_MOM12
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
SELECT wzs.code as symbol,
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
         join momentum m on m.code = t.symbol
         join momentum_risk_adj mra on mra.code = t.symbol
         join windsored_z_score wzs on wzs.code = t.symbol
         join z_score ON z_score.code = wzs.code
         join momentum_risk_adj_stats ON momentum_risk_adj_stats.gic_sector = t.gic_sector
