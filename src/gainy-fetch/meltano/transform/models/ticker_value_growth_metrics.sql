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

/* EGRSF https://www.msci.com/eqb/methodology/meth_docs/MSCI_Feb13_Fundamental_Data.pdf */

with latest_yearly_earning_trend as
         (
             SELECT et.symbol,
                    et.growth
             from {{ ref('earnings_trend') }} et
                      LEFT JOIN {{ ref('earnings_trend') }} AS et_next
                                ON et_next.symbol = et.symbol AND et_next.period = '0y' AND
                                   et_next.date::timestamp > et.date::timestamp
             WHERE et.period = '0y'
               AND et_next.symbol IS NULL
         ),
     hist_eps_growth as (
         SELECT eh0.symbol,
                cbrt(eh0.eps_actual / eh1.eps_actual) - 1 as value
         from {{ ref('earnings_history') }} eh0
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
         from {{ ref('financials_income_statement_quarterly') }} fisq0
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
                                   left join earnings_annual ea_next
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
                          FROM {{ ref('earnings_trend') }} et
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
             SELECT eps12.symbol,
                    eps0,
                    eps1,
                    eps2,
                    cur_month.value                                                        as M,
                    eps12.f                                                                AS eps12_f,
                    eps12.b                                                                AS eps12_b,
                    CASE WHEN abs(eps12.b) > 0 THEN (eps12.f - eps12.b) / abs(eps12.b) END as value
             from eps12
                      JOIN eps_metrics ON eps_metrics.symbol = eps12.symbol
                      JOIN cur_month ON true
         ),
     vg_metrics as
         (
             select f.code,
                    t.gic_sector,
                 /* growth */
                    egrsf.value                                                             as st_fwd_eps,
                    (h.diluted_eps_ttm / h.book_value) *
                    (1 - (f.splitsdividends ->> 'PayoutRatio')::float)                      as cur_internal_growth_rate,
                    heg.value                                                               as hist_eps_growth,
                    hsg.value                                                               as hist_sales_growth,
                 /* value */
                    h.book_value / hp.close                                                 as bvp,
                    CASE WHEN abs(v.forward_pe::float) > 0 THEN 1 / v.forward_pe::float END as fwd_ep,
                    (f.splitsdividends ->> 'ForwardAnnualDividendYield')::float             as dividend_yield

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
             SELECT gic_sector,
                 /*AVG(lt_fwd_eps)                      as avg_lt_fwd_eps,*/
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
             GROUP BY vgm.gic_sector
         ),
     z_score as
         (
             SELECT vg_metrics.code,
                 /*(lt_fwd_eps - avg_lt_fwd_eps) / stddev_lt_fwd_eps                      as z_score_lt_fwd_eps,*/
                    CASE
                        WHEN ABS(stddev_st_fwd_eps) > 0
                            THEN (st_fwd_eps - avg_st_fwd_eps) / stddev_st_fwd_eps END                      as z_score_st_fwd_eps,
                    CASE
                        WHEN ABS(stddev_cur_internal_growth_rate) > 0 THEN
                                (cur_internal_growth_rate - avg_cur_internal_growth_rate) /
                                stddev_cur_internal_growth_rate END                                         as z_score_cur_internal_growth_rate,
                    CASE
                        WHEN ABS(stddev_hist_eps_growth) > 0
                            THEN (hist_eps_growth - avg_hist_eps_growth) / stddev_hist_eps_growth END       as z_score_hist_eps_growth,
                    CASE
                        WHEN ABS(stddev_hist_sales_growth) > 0
                            THEN (hist_sales_growth - avg_hist_sales_growth) / stddev_hist_sales_growth END as z_score_hist_sales_growth,
                    CASE WHEN ABS(stddev_bvp) > 0 THEN (bvp - avg_bvp) / stddev_bvp END                     as z_score_bvp,
                    CASE
                        WHEN ABS(stddev_fwd_ep) > 0
                            THEN (fwd_ep - avg_fwd_ep) / stddev_fwd_ep END                                  as z_score_fwd_ep,
                    CASE
                        WHEN ABS(stddev_dividend_yield) > 0
                            THEN (dividend_yield - avg_dividend_yield) / stddev_dividend_yield END          as z_score_dividend_yield
             from vg_metrics
                      JOIN vg_metrics_stats ON vg_metrics_stats.gic_sector = vg_metrics.gic_sector
         ),
     windsored_z_score as
         (
             SELECT zs.code,
                 /*GREATEST(-3, LEAST(3, z_score_lt_fwd_eps))               as windsored_z_score_lt_fwd_eps,*/
                    CASE
                        WHEN z_score_st_fwd_eps IS NOT NULL
                            THEN GREATEST(-3, LEAST(3, z_score_st_fwd_eps)) END                          as windsored_z_score_st_fwd_eps,
                    CASE
                        WHEN z_score_cur_internal_growth_rate IS NOT NULL
                            THEN GREATEST(-3, LEAST(3, z_score_cur_internal_growth_rate)) END            as windsored_z_score_cur_internal_growth_rate,
                    CASE
                        WHEN z_score_hist_eps_growth IS NOT NULL
                            THEN GREATEST(-3, LEAST(3, z_score_hist_eps_growth)) END                     as windsored_z_score_hist_eps_growth,
                    CASE
                        WHEN z_score_hist_sales_growth IS NOT NULL
                            THEN GREATEST(-3, LEAST(3, z_score_hist_sales_growth)) END                   as windsored_z_score_hist_sales_growth,
                    CASE
                        WHEN z_score_bvp IS NOT NULL
                            THEN GREATEST(-3, LEAST(3, z_score_bvp)) END                                 as windsored_z_score_bvp,
                    CASE
                        WHEN z_score_fwd_ep IS NOT NULL
                            THEN GREATEST(-3, LEAST(3, z_score_fwd_ep)) END                              as windsored_z_score_fwd_ep,
                    CASE
                        WHEN z_score_dividend_yield IS NOT NULL
                            THEN GREATEST(-3, LEAST(3, z_score_dividend_yield)) END                      as windsored_z_score_dividend_yield
             from z_score zs
         )
SELECT t.symbol,

       eps0,
       eps1,
       eps2,
       M,
       eps12_f,
       eps12_b,

       st_fwd_eps,
       cur_internal_growth_rate,
       hist_eps_growth,
       hist_sales_growth,
       bvp,
       fwd_ep,
       dividend_yield,

       avg_st_fwd_eps,
       stddev_st_fwd_eps,
       avg_cur_internal_growth_rate,
       stddev_cur_internal_growth_rate,
       avg_hist_eps_growth,
       stddev_hist_eps_growth,
       avg_hist_sales_growth,
       stddev_hist_sales_growth,
       avg_bvp,
       stddev_bvp,
       avg_fwd_ep,
       stddev_fwd_ep,
       avg_dividend_yield,
       stddev_dividend_yield,

       z_score_st_fwd_eps,
       z_score_cur_internal_growth_rate,
       z_score_hist_eps_growth,
       z_score_hist_sales_growth,
       z_score_bvp,
       z_score_fwd_ep,
       z_score_dividend_yield,

       windsored_z_score_st_fwd_eps,
       windsored_z_score_cur_internal_growth_rate,
       windsored_z_score_hist_eps_growth,
       windsored_z_score_hist_sales_growth,
       windsored_z_score_bvp,
       windsored_z_score_fwd_ep,
       windsored_z_score_dividend_yield,

       (windsored_z_score_st_fwd_eps +
        windsored_z_score_cur_internal_growth_rate +
        windsored_z_score_hist_eps_growth +
        windsored_z_score_hist_sales_growth) / 4 as growth_score,
       (windsored_z_score_bvp +
        windsored_z_score_fwd_ep +
        windsored_z_score_dividend_yield) / 3    as value_score
from {{ ref('tickers') }} t
         JOIN egrsf ON egrsf.symbol = t.symbol
         JOIN vg_metrics ON vg_metrics.code = t.symbol
         JOIN vg_metrics_stats ON vg_metrics_stats.gic_sector = t.gic_sector
         JOIN z_score ON z_score.code = t.symbol
         JOIN windsored_z_score ON windsored_z_score.code = t.symbol
