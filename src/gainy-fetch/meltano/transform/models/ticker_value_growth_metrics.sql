{{
  config(
    materialized = "table",
    dist = "symbol",
    post_hook=[
      index(this, 'symbol', true),
    ]
  )
}}

/* EGRSF https://www.msci.com/eqb/methodology/meth_docs/MSCI_Feb13_Fundamental_Data.pdf */

with tickers as (select * from {{ ref('tickers') }}),
     earnings_trend as (select * from {{ ref('earnings_trend') }}),
     earnings_annual as (select * from {{ ref('earnings_annual') }}),
     highlights as (select * from {{ ref('highlights') }}),
     historical_eps_growth as (select * from {{ ref('historical_eps_growth') }}),
     historical_sales_growth as (select * from {{ ref('historical_sales_growth') }}),
     valuation as (select * from {{ ref('valuation') }}),
     hsg_extended as
         (
             select distinct on (hp.code) hp.close as quartal_end_price, hsg.*
             from historical_sales_growth hsg
                      JOIN historical_prices hp ON hp.code = hsg.symbol AND hp.date::date <= hsg.updated_at AND
                                                   hp.date::date >= hsg.updated_at - interval '1 week'
             order by hp.code, hp.date::date DESC
         ),
     latest_yearly_earning_trend as
         (
             SELECT et.symbol,
                    et.growth
             from earnings_trend et
                      LEFT JOIN earnings_trend AS et_next
                                ON et_next.symbol = et.symbol AND et_next.period = '0y' AND
                                   et_next.date::timestamp > et.date::timestamp
             WHERE et.period = '0y'
               AND et_next.symbol IS NULL
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
                          FROM earnings_trend et
                                   left join earnings_trend et_next
                                             ON et_next.symbol = et.symbol AND
                                                et_next.date::timestamp >
                                                et.date::timestamp AND
                                                et.period = et_next.period
                                   join earnings_trend et_next_year
                                        ON et_next_year.symbol = et.symbol AND et_next_year.period = '+1y'
                                   left join earnings_trend et_next_year_next
                                             ON et_next_year_next.symbol = et.symbol AND
                                                et_next_year_next.period = '+1y' AND
                                                et_next_year_next.date::timestamp > et_next_year.date::timestamp
                                   JOIN eps_actual ON eps_actual.symbol = et.symbol
                          WHERE et_next.symbol IS NULL
                            AND et_next_year_next.symbol IS NULL
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
                    egrsf.value                                                                       as st_fwd_eps,
                    CASE
                        WHEN ABS(h.book_value) > 0
                            THEN (h.diluted_eps_ttm / h.book_value) *
                                 (1 - (f.splitsdividends ->> 'PayoutRatio')::float)
                        END                                                                           as cur_internal_growth_rate,
                    heg.value                                                                         as hist_eps_growth,
                    hsg.value                                                                         as hist_sales_growth,
                 /* value */
                    CASE WHEN hsg.quartal_end_price > 0 THEN h.book_value / hsg.quartal_end_price END as bvp,
                    CASE
                        WHEN abs(v.forward_pe::float) > 0
                            THEN 1 / v.forward_pe::float END                                          as fwd_ep,
                    (f.splitsdividends ->> 'ForwardAnnualDividendYield')::float                       as dividend_yield
             from fundamentals f
                      JOIN tickers t
                           ON t.symbol = f.code
                      JOIN highlights h ON h.symbol = f.code
                      JOIN egrsf ON egrsf.symbol = f.code
                      JOIN latest_yearly_earning_trend lyet ON lyet.symbol = f.code
                      JOIN historical_eps_growth heg ON heg.symbol = f.code
                      JOIN hsg_extended hsg ON hsg.symbol = f.code
                      JOIN valuation v ON v.symbol = f.code
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
                            THEN GREATEST(-3, LEAST(3, z_score_st_fwd_eps)) END               as windsored_z_score_st_fwd_eps,
                    CASE
                        WHEN z_score_cur_internal_growth_rate IS NOT NULL
                            THEN GREATEST(-3, LEAST(3, z_score_cur_internal_growth_rate)) END as windsored_z_score_cur_internal_growth_rate,
                    CASE
                        WHEN z_score_hist_eps_growth IS NOT NULL
                            THEN GREATEST(-3, LEAST(3, z_score_hist_eps_growth)) END          as windsored_z_score_hist_eps_growth,
                    CASE
                        WHEN z_score_hist_sales_growth IS NOT NULL
                            THEN GREATEST(-3, LEAST(3, z_score_hist_sales_growth)) END        as windsored_z_score_hist_sales_growth,
                    CASE
                        WHEN z_score_bvp IS NOT NULL
                            THEN GREATEST(-3, LEAST(3, z_score_bvp)) END                      as windsored_z_score_bvp,
                    CASE
                        WHEN z_score_fwd_ep IS NOT NULL
                            THEN GREATEST(-3, LEAST(3, z_score_fwd_ep)) END                   as windsored_z_score_fwd_ep,
                    CASE
                        WHEN z_score_dividend_yield IS NOT NULL
                            THEN GREATEST(-3, LEAST(3, z_score_dividend_yield)) END           as windsored_z_score_dividend_yield
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
from tickers t
         JOIN egrsf ON egrsf.symbol = t.symbol
         JOIN vg_metrics ON vg_metrics.code = t.symbol
         JOIN vg_metrics_stats ON vg_metrics_stats.gic_sector = t.gic_sector
         JOIN z_score ON z_score.code = t.symbol
         JOIN windsored_z_score ON windsored_z_score.code = t.symbol
