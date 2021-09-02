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

with momentum_metrics as
         (
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
                                 hp0.close / hp1.close - 1 - settings.local_risk_free_rate AS MOM2,
                                 hp0.close / hp2.close - 1 - settings.local_risk_free_rate AS MOM12
                          from fundamentals f
                                   join settings ON 1 = 1
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
                                 m.MOM2 / s3y.value  as Risk_Adj_MOM2,
                                 m.MOM12 / s3y.value as Risk_Adj_MOM12
                          from momentum m
                                   JOIN stddev_3_years s3y ON s3y.code = m.code
                      ),
                  momentum_risk_adj_stats as
                      (
                          SELECT AVG(Risk_Adj_MOM2)     as AVG_Risk_Adj_MOM2,
                                 stddev(Risk_Adj_MOM2)  as StdDev_Risk_Adj_MOM2,
                                 AVG(Risk_Adj_MOM12)    as AVG_Risk_Adj_MOM12,
                                 stddev(Risk_Adj_MOM12) as StdDev_Risk_Adj_MOM12
                          from momentum_risk_adj mra
                      ),
                  z_score as
                      (
                          SELECT mra.code,
                                 (Risk_Adj_MOM2 - mras.AVG_Risk_Adj_MOM2) / mras.StdDev_Risk_Adj_MOM2    as Z_Score_MOM2,
                                 (Risk_Adj_MOM12 - mras.AVG_Risk_Adj_MOM12) / mras.StdDev_Risk_Adj_MOM12 as Z_Score_MOM12
                          from momentum_risk_adj mra
                                   join momentum_risk_adj_stats mras ON true
                      ),
                  windsored_z_score as
                      (
                          SELECT zs.code,
                                 GREATEST(-3, LEAST(3, Z_Score_MOM2))  as Windsored_Z_Score_MOM2,
                                 GREATEST(-3, LEAST(3, Z_Score_MOM12)) as Windsored_Z_Score_MOM12
                          from z_score zs
                      )
             SELECT code,
                    (wzs.Windsored_Z_Score_MOM2 + wzs.Windsored_Z_Score_MOM12) / 2 as combined_momentum_score
             FROM windsored_z_score wzs
         ),
     vg_metrics as
         (
             with latest_yearly_earning_trend as
                      (
                          SELECT et.symbol,
                                 et.growth
                          from earnings_trend et
                                   LEFT JOIN {{ ref('earnings_trend') }} AS et_next
                                             ON et_next.symbol = et.symbol AND et_next.period = '0y' AND
                                                et_next.date::timestamp > et.date::timestamp
                          WHERE et.period = '0y'
                            AND et_next.symbol IS NULL
                      ),
                  hist_eps_growth as (
                      SELECT eh0.symbol,
                             CASE
                                 WHEN eh0.eps_actual > 0 AND eh1.eps_actual > 0
                                     THEN POW(eh0.eps_actual / eh1.eps_actual, 1 / 3.0) - 1
                                 ELSE NULL END as value
                      from earnings_history eh0
                               LEFT JOIN {{ ref('earnings_history') }} eh0_next
                                         ON eh0_next.symbol = eh0.symbol AND
                                            eh0_next.date::timestamp > eh0.date::timestamp AND
                                            eh0_next.date::timestamp < NOW()
                               JOIN {{ ref('earnings_history') }} eh1
                                    ON eh1.symbol = eh0.symbol AND eh1.date::timestamp < NOW() - interval '3 years'
                               LEFT JOIN {{ ref('earnings_history') }} eh1_next ON eh1_next.symbol = eh0.symbol AND
                                                                      eh1_next.date::timestamp <
                                                                      NOW() - interval '3 years' AND
                                                                      eh1_next.date::timestamp > eh1.date::timestamp
                      WHERE eh0.date::timestamp < NOW()
                        AND eh0_next.symbol IS NULL
                        AND eh1_next.symbol IS NULL
                  ),
                  hist_sales_growth as (
                      SELECT fisq0.symbol,
                             fisq0.date::timestamp as date,
                             CASE
                                 WHEN fisq0.total_revenue > 0 AND fisq1.total_revenue > 0
                                     THEN POW(fisq0.total_revenue / fisq1.total_revenue, 1 / 3.0) - 1
                                 ELSE NULL END     as value
                      from financials_income_statement_quarterly fisq0
                               LEFT JOIN {{ ref('financials_income_statement_quarterly') }} fisq0_next
                                         ON fisq0_next.symbol = fisq0.symbol AND
                                            fisq0_next.date::timestamp > fisq0.date::timestamp AND
                                            fisq0_next.date::timestamp < NOW()
                               JOIN {{ ref('financials_income_statement_quarterly') }} fisq1
                                    ON fisq1.symbol = fisq0.symbol AND
                                       fisq1.date::timestamp < NOW() - interval '3 years'
                               LEFT JOIN {{ ref('financials_income_statement_quarterly') }} fisq1_next
                                         ON fisq1_next.symbol = fisq0.symbol AND
                                            fisq1_next.date::timestamp < NOW() - interval '3 years' AND
                                            fisq1_next.date::timestamp > fisq1.date::timestamp
                      WHERE fisq0.date::timestamp < NOW()
                        AND fisq0_next.symbol IS NULL
                        AND fisq1_next.symbol IS NULL
                  ),
                  vg_metrics as
                      (
                          with latest_yearly_earning_trend as
                                   (
                                       SELECT et.symbol,
                                              et.growth
                                       from earnings_trend et
                                                LEFT JOIN {{ ref('earnings_trend') }} AS et_next
                                                          ON et_next.symbol = et.symbol AND et_next.period = '0y' AND
                                                             et_next.date::timestamp > et.date::timestamp
                                       WHERE et.period = '0y'
                                         AND et_next.symbol IS NULL
                                   ),
                               hist_eps_growth as (
                                   SELECT eh0.symbol,
                                          CASE
                                              WHEN eh0.eps_actual > 0 AND eh1.eps_actual > 0
                                                  THEN POW(eh0.eps_actual / eh1.eps_actual, 1 / 3.0) - 1
                                              ELSE NULL END as value
                                   from earnings_history eh0
                                            LEFT JOIN {{ ref('earnings_history') }} eh0_next
                                                      ON eh0_next.symbol = eh0.symbol AND
                                                         eh0_next.date::timestamp > eh0.date::timestamp AND
                                                         eh0_next.date::timestamp < NOW()
                                            JOIN {{ ref('earnings_history') }} eh1
                                                 ON eh1.symbol = eh0.symbol AND
                                                    eh1.date::timestamp < NOW() - interval '3 years'
                                            LEFT JOIN {{ ref('earnings_history') }} eh1_next ON eh1_next.symbol = eh0.symbol AND
                                                                                   eh1_next.date::timestamp <
                                                                                   NOW() - interval '3 years' AND
                                                                                   eh1_next.date::timestamp > eh1.date::timestamp
                                   WHERE eh0.date::timestamp < NOW()
                                     AND eh0_next.symbol IS NULL
                                     AND eh1_next.symbol IS NULL
                               ),
                               hist_sales_growth as (
                                   SELECT fisq0.symbol,
                                          fisq0.date::timestamp as date,
                                          CASE
                                              WHEN fisq0.total_revenue > 0 AND fisq1.total_revenue > 0
                                                  THEN POW(fisq0.total_revenue / fisq1.total_revenue, 1 / 3.0) - 1
                                              ELSE NULL END     as value
                                   from financials_income_statement_quarterly fisq0
                                            LEFT JOIN {{ ref('financials_income_statement_quarterly') }} fisq0_next
                                                      ON fisq0_next.symbol = fisq0.symbol AND
                                                         fisq0_next.date::timestamp > fisq0.date::timestamp AND
                                                         fisq0_next.date::timestamp < NOW()
                                            JOIN {{ ref('financials_income_statement_quarterly') }} fisq1
                                                 ON fisq1.symbol = fisq0.symbol AND
                                                    fisq1.date::timestamp < NOW() - interval '3 years'
                                            LEFT JOIN {{ ref('financials_income_statement_quarterly') }} fisq1_next
                                                      ON fisq1_next.symbol = fisq0.symbol AND
                                                         fisq1_next.date::timestamp < NOW() - interval '3 years' AND
                                                         fisq1_next.date::timestamp > fisq1.date::timestamp
                                   WHERE fisq0.date::timestamp < NOW()
                                     AND fisq0_next.symbol IS NULL
                                     AND fisq1_next.symbol IS NULL
                               ),
                               egrsf as
                                   (
                                       with eps_actual as
                                                (
                                                    select ea.symbol,
                                                           ea.eps_actual as value
                                                    from earnings_annual ea
                                                             left join {{ ref('earnings_annual') }} ea_next
                                                                       ON ea_next.symbol = ea.symbol AND
                                                                          ea_next.date::timestamp > ea.date::timestamp
                                                    WHERE ea_next.date IS NULL
                                                ),
                                            eps_metrics as
                                                (
                                                    SELECT et.symbol,
                                                           eps_actual.value                   as eps0,
                                                           et.earnings_estimate_avg           as eps1,
                                                           et_next_year.earnings_estimate_avg as eps2
                                                    FROM earnings_trend et
                                                             left join {{ ref('earnings_trend') }} et_next
                                                                       ON et_next.symbol = et.symbol AND
                                                                          et_next.date::timestamp >
                                                                          et.date::timestamp AND
                                                                          et.period = et_next.period
                                                             join {{ ref('earnings_trend') }} et_next_year
                                                                  ON et_next_year.symbol = et.symbol AND et_next_year.period = '+1y'
                                                             JOIN eps_actual ON eps_actual.symbol = et.symbol
                                                    WHERE et_next.symbol IS NULL
                                                      AND et.period = '0y'
                                                ),
                                            cur_month as (SELECT date_part('month', NOW()) as value),
                                            eps12 as
                                                (
                                                    SELECT symbol,
                                                           cur_month.value,
                                                           (cur_month.value * eps0 + (12 - cur_month.value) * eps1) / 12 as b,
                                                           (cur_month.value * eps1 + (12 - cur_month.value) * eps2) / 12 as f
                                                    from eps_metrics
                                                             JOIN cur_month ON true
                                                )
                                       SELECT symbol,
                                              (eps12.f - eps12.b) / abs(eps12.b) as value
                                       from eps12
                                   )
                          select f.code,
                              /* growth */
                                 egrsf.value                                                                  as st_fwd_eps,
                                 (h.diluted_eps_ttm / h.book_value) *
                                 (1 - (f.splitsdividends ->> 'PayoutRatio')::float)                           as cur_internal_growth_rate,
                                 heg.value                                                                    as hist_eps_growth,
                                 hsg.value                                                                    as hist_sales_growth,
                              /* value */
                                 h.book_value / hp.close                                                      as bvp,
                                 CASE WHEN v.forward_pe::float = 0 THEN null ELSE 1 / v.forward_pe::float END as fwd_ep,
                                 (f.splitsdividends ->> 'ForwardAnnualDividendYield')::float                  as dividend_yield

                          from fundamentals f
                                   JOIN {{ ref('tickers') }} t
                                        ON t.symbol = f.code
                                   JOIN {{ ref('highlights') }} h ON h.symbol = f.code
                                   JOIN egrsf ON egrsf.symbol = f.code
                                   JOIN latest_yearly_earning_trend lyet ON lyet.symbol = f.code
                                   JOIN hist_eps_growth heg ON heg.symbol = f.code
                                   JOIN hist_sales_growth hsg ON hsg.symbol = f.code
                                   JOIN historical_prices hp ON hp.code = f.code AND hp.date::timestamp <= hsg.date
                                   LEFT JOIN historical_prices hp_next
                                             ON hp_next.code = f.code AND hp_next.date::timestamp <= hsg.date AND
                                                hp_next.date::timestamp > hp.date::timestamp
                                   JOIN {{ ref('valuation') }} v ON v.symbol = f.code
                          WHERE hp_next.date IS NULL
                      ),
                  vg_metrics_stats as
                      (
                          SELECT /*AVG(lt_fwd_eps)                      as avg_lt_fwd_eps,*/
                              /*stddev_pop(lt_fwd_eps)               as stddev_lt_fwd_eps,*/
                              AVG(st_fwd_eps)                      as avg_st_fwd_eps,
                              stddev_pop(st_fwd_eps)               as stddev_st_fwd_eps,
                              AVG(cur_internal_growth_rate)        as avg_cur_internal_growth_rate,
                              stddev_pop(cur_internal_growth_rate) as stddev_cur_internal_growth_rate,
                              AVG(hist_eps_growth)                 as avg_hist_eps_growth,
                              stddev_pop(hist_eps_growth)          as stddev_hist_eps_growth,
                              AVG(hist_sales_growth)               as avg_hist_sales_growth,
                              stddev_pop(hist_sales_growth)        as stddev_hist_sales_growth,
                              AVG(bvp)                             as avg_bvp,
                              stddev_pop(bvp)                      as stddev_bvp,
                              AVG(fwd_ep)                          as avg_fwd_ep,
                              stddev_pop(fwd_ep)                   as stddev_fwd_ep,
                              AVG(dividend_yield)                  as avg_dividend_yield,
                              stddev_pop(dividend_yield)           as stddev_dividend_yield
                          from vg_metrics vgm
                      ),
                  z_score as
                      (
                          SELECT vg_metrics.code,
                              /*(lt_fwd_eps - avg_lt_fwd_eps) / stddev_lt_fwd_eps                      as z_score_lt_fwd_eps,*/
                                 (st_fwd_eps - avg_st_fwd_eps) / stddev_st_fwd_eps                      as z_score_st_fwd_eps,
                                 (cur_internal_growth_rate - avg_cur_internal_growth_rate) /
                                 stddev_cur_internal_growth_rate                                        as z_score_cur_internal_growth_rate,
                                 (hist_eps_growth - avg_hist_eps_growth) / stddev_hist_eps_growth       as z_score_hist_eps_growth,
                                 (hist_sales_growth - avg_hist_sales_growth) / stddev_hist_sales_growth as z_score_hist_sales_growth,
                                 (bvp - avg_bvp) / stddev_bvp                                           as z_score_bvp,
                                 (fwd_ep - avg_fwd_ep) / stddev_fwd_ep                                  as z_score_fwd_ep,
                                 (dividend_yield - avg_dividend_yield) / stddev_dividend_yield          as z_score_dividend_yield
                          from vg_metrics
                                   JOIN vg_metrics_stats ON true
                      ),
                  windsored_z_score as
                      (
                          SELECT zs.code,
                              /*GREATEST(-3, LEAST(3, z_score_lt_fwd_eps))               as windsored_z_score_lt_fwd_eps,*/
                                 GREATEST(-3, LEAST(3, z_score_st_fwd_eps))               as windsored_z_score_st_fwd_eps,
                                 GREATEST(-3, LEAST(3, z_score_cur_internal_growth_rate)) as windsored_z_score_cur_internal_growth_rate,
                                 GREATEST(-3, LEAST(3, z_score_hist_eps_growth))          as windsored_z_score_hist_eps_growth,
                                 GREATEST(-3, LEAST(3, z_score_hist_sales_growth))        as windsored_z_score_hist_sales_growth,
                                 GREATEST(-3, LEAST(3, z_score_bvp))                      as windsored_z_score_bvp,
                                 GREATEST(-3, LEAST(3, z_score_fwd_ep))                   as windsored_z_score_fwd_ep,
                                 GREATEST(-3, LEAST(3, z_score_dividend_yield))           as windsored_z_score_dividend_yield
                          from z_score zs
                      )
             SELECT code,
                    (/*windsored_z_score_lt_fwd_eps + */windsored_z_score_st_fwd_eps +
                                                        windsored_z_score_cur_internal_growth_rate +
                                                        windsored_z_score_hist_eps_growth +
                                                        windsored_z_score_hist_sales_growth) / 4 as growth_score,
                    (windsored_z_score_bvp + windsored_z_score_fwd_ep + windsored_z_score_dividend_yield) /
                    3                                                                            as value_score
             from windsored_z_score
         )
select f.code                                          as symbol,
       momentum_metrics.combined_momentum_score,
       vg_metrics.growth_score,
       vg_metrics.value_score,
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
         JOIN momentum_metrics ON momentum_metrics.code = f.code
         JOIN vg_metrics ON vg_metrics.code = f.code