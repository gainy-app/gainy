{{
  config(
    materialized = "table",
    post_hook=[
      index(this, 'symbol', true),
    ]
  )
}}

with highlights as (select * from {{ ref('highlights') }}),
     valuation as (select * from {{ ref('valuation') }}),
     technicals as (select * from {{ ref('technicals') }}),
     ticker_shares_stats as (select * from {{ ref('ticker_shares_stats') }}),
     financials_income_statement_quarterly as (select * from {{ ref('financials_income_statement_quarterly') }}),
     earnings_trend as (select * from {{ ref('earnings_trend') }}),
     earnings_history as (select * from {{ ref('earnings_history') }}),
     earnings_annual as (select * from {{ ref('earnings_annual') }}),
     raw_eod_options as (SELECT * FROM {{ source('eod', 'eod_options') }}),
     marked_prices as
         (
             select distinct on (hp.code, hp.period) *
             from (
                      select *,
                             case
                                 when hp."date" <= hp.cur_date - interval '3 month' then '3m'
                                 when hp."date" <= hp.cur_date - interval '1 month' then '1m'
                                 when hp."date" <= hp.cur_date - interval '10 days' then '10d'
                                 when hp."date" <= hp.cur_date then '0d'
                                 end as period
                      from (
                               select code,
                                      "date"::date,
                                      adjusted_close::numeric                                                 as price,
                                      volume,
                                      first_value("date"::date) over (partition by code order by "date" desc) as cur_date
                               from historical_prices
                           ) hp
                  ) hp
             where period is not null
             order by hp.code, hp.period, hp.date desc
         ),
     today_price as (select * from marked_prices mp where period = '0d'),
     trading_stats as
         (
             with avg_volume_10d as
                      (
                          select hp.code,
                                 avg(hp.volume) as value
                          from historical_prices hp
                                   join marked_prices mp on mp.code = hp.code and mp.period = '10d'
                          where hp.date > mp.date
                          group by hp.code
                      ),
                  avg_volume_90d as
                      (
                          select hp.code,
                                 avg(hp.volume) as value
                          from historical_prices hp
                                   join marked_prices mp on mp.code = hp.code and mp.period = '3m'
                          where hp.date > mp.date
                          group by hp.code
                      ),
                  historical_volatility as
                      (
                          with volatility as
                                   (
                                       select code,
                                              date,
                                              stddev_pop(adjusted_close)
                                              OVER (partition by code ORDER BY date desc ROWS BETWEEN CURRENT ROW AND 255 FOLLOWING) as absolute_historical_volatility_adjusted,
                                              stddev_pop(adjusted_close)
                                              OVER (partition by code ORDER BY date desc ROWS BETWEEN CURRENT ROW AND 255 FOLLOWING) /
                                              adjusted_close                                                                         as relative_historical_volatility_adjusted
                                       from historical_prices
                                       where date > now() - interval '2 year'
                                       order by code, date desc
                                   ),
                               volatility_current as
                                   (
                                       select distinct on (code) code,
                                                                 volatility.absolute_historical_volatility_adjusted as absolute_historical_volatility_adjusted_current,
                                                                 volatility.relative_historical_volatility_adjusted as relative_historical_volatility_adjusted_current
                                       from volatility
                                       where date > now() - interval '1 year'
                                       order by code, date desc
                                   ),
                               volatility_stats as
                                   (
                                       select code,
                                              min(volatility.absolute_historical_volatility_adjusted) as absolute_historical_volatility_adjusted_min_1y,
                                              max(volatility.absolute_historical_volatility_adjusted) as absolute_historical_volatility_adjusted_max_1y,
                                              min(volatility.relative_historical_volatility_adjusted) as relative_historical_volatility_adjusted_min_1y,
                                              max(volatility.relative_historical_volatility_adjusted) as relative_historical_volatility_adjusted_max_1y
                                       from volatility
                                       where date > now() - interval '1 year'
                                       group by volatility.code
                                   )
                          select volatility_current.*,
                                 volatility_stats.absolute_historical_volatility_adjusted_min_1y,
                                 volatility_stats.absolute_historical_volatility_adjusted_max_1y,
                                 volatility_stats.relative_historical_volatility_adjusted_min_1y,
                                 volatility_stats.relative_historical_volatility_adjusted_max_1y
                          from volatility_current
                                   join volatility_stats on volatility_stats.code = volatility_current.code
                      ),
                  implied_volatility as
                      (
                          select code,
                                 avg(impliedvolatility) as value
                          from raw_eod_options
                          group by code
                      )
             select t.symbol,
                    avg_volume_10d.value::double precision as avg_volume_10d,
                    ticker_shares_stats.short_percent_outstanding::double precision,
                    ticker_shares_stats.shares_outstanding::bigint,
                    avg_volume_90d.value::double precision as avg_volume_90d,
                    ticker_shares_stats.shares_float::bigint,
                    ticker_shares_stats.short_ratio::double precision,
                    technicals.beta::double precision,
                    historical_volatility.absolute_historical_volatility_adjusted_current,
                    historical_volatility.relative_historical_volatility_adjusted_current,
                    historical_volatility.absolute_historical_volatility_adjusted_min_1y,
                    historical_volatility.absolute_historical_volatility_adjusted_max_1y,
                    historical_volatility.relative_historical_volatility_adjusted_min_1y,
                    historical_volatility.relative_historical_volatility_adjusted_max_1y,
                    implied_volatility.value               as implied_volatility
             from tickers t
                      left join ticker_shares_stats on t.symbol = ticker_shares_stats.symbol
                      left join technicals on t.symbol = technicals.symbol
                      left join avg_volume_10d on t.symbol = avg_volume_10d.code
                      left join avg_volume_90d on t.symbol = avg_volume_90d.code
                      left join historical_volatility on t.symbol = historical_volatility.code
                      left join implied_volatility on t.symbol = implied_volatility.code
         ),
     growth_stats as (
         with expanded_income_statement_quarterly as
                  (
                      select *,
                             sum(total_revenue)
                             OVER (partition by symbol ORDER BY date desc ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING) as total_revenue_ttm,
                             sum(ebitda)
                             OVER (partition by symbol ORDER BY date desc ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING) as ebitda_ttm
                      from financials_income_statement_quarterly
                      order by symbol, date desc
                  ),
              expanded_earnings_history as
                  (
                      select *,
                             sum(eps_actual)
                             OVER (partition by symbol ORDER BY date desc ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING) as eps_actual_ttm
                      from earnings_history
                      order by symbol, date desc
                  ),
              ebitda_growth_yoy as
                  (
                      select distinct on (symbol) *,
                                                  ebitda_ttm /
                                                  last_value(case when ebitda_ttm > 0 then ebitda_ttm end)
                                                  OVER (partition by symbol ORDER BY date desc ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING) -
                                                  1 as value
                      from expanded_income_statement_quarterly
                      order by symbol
                  ),
              eps_actual_growth_yoy as
                  (
                      select distinct on (symbol) *,
                                                  eps_actual_ttm /
                                                  last_value(case when eps_actual_ttm > 0 then eps_actual_ttm end)
                                                  OVER (partition by symbol ORDER BY date desc ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING) -
                                                  1 as value
                      from expanded_earnings_history
                      where eps_actual is not null
                      order by symbol
                  ),
              earnings_trend_0y as
                  (
                      select distinct on (symbol) * from earnings_trend where period = '0y' order by symbol, date desc
                  ),
              earnings_trend_1y as
                  (
                      select distinct on (symbol) * from earnings_trend where period = '+1y' order by symbol, date desc
                  ),
              latest_expanded_income_statement_quarterly as
                  (
                      select distinct on (symbol) * from expanded_income_statement_quarterly order by symbol, date desc
                  ),
              latest_expanded_earnings_history_with_eps_actual as
                  (
                      select distinct on (symbol) *
                      from expanded_earnings_history
                      where eps_actual is not null
                      order by symbol, date desc
                  ),
              latest_earnings_annual as
                  (
                      select distinct on (symbol) *
                      from earnings_annual
                      order by symbol, date desc
                  )
         select tickers.symbol,
                highlights.quarterly_revenue_growth_yoy::double precision        as revenue_growth_yoy,
                COALESCE(earnings_trend_1y.revenue_estimate_avg, earnings_trend_0y.revenue_estimate_avg) /
                latest_expanded_income_statement_quarterly.total_revenue_ttm - 1 as revenue_growth_fwd,
                ebitda_growth_yoy.value                                          as ebitda_growth_yoy,
                eps_actual_growth_yoy.value                                      as eps_growth_yoy,
                -- SeekingAlpha: The forward growth rate is a compounded annual growth rate from the most recently completed fiscal year's EPS (FY(-1)) to analysts' consensus EPS estimates for two fiscal years forward (FY2).
                case
                    when latest_earnings_annual.eps_actual > 0 then
                        coalesce(sqrt(earnings_trend_1y.earnings_estimate_avg / latest_earnings_annual.eps_actual) - 1,
                                 earnings_trend_0y.earnings_estimate_avg / latest_earnings_annual.eps_actual - 1)
                    end                                                          as eps_growth_fwd
         from tickers
                  left join highlights on highlights.symbol = tickers.symbol
                  left join ebitda_growth_yoy on ebitda_growth_yoy.symbol = tickers.symbol
                  left join eps_actual_growth_yoy on eps_actual_growth_yoy.symbol = tickers.symbol
                  left join earnings_trend_0y on earnings_trend_0y.symbol = tickers.symbol
                  left join earnings_trend_1y on earnings_trend_1y.symbol = tickers.symbol
                  left join latest_earnings_annual on latest_earnings_annual.symbol = tickers.symbol
                  left join latest_expanded_income_statement_quarterly
                            on latest_expanded_income_statement_quarterly.symbol = tickers.symbol
                  left join latest_expanded_earnings_history_with_eps_actual
                            on latest_expanded_earnings_history_with_eps_actual.symbol = tickers.symbol
     )
select DISTINCT ON
    (t.symbol) t.symbol,

    /* Selected */
               highlights.market_capitalization::bigint,
               case
                   when today_price.price > 0
                       then (select mp.price from marked_prices mp where code = highlights.symbol and period = '1m') /
                            today_price.price
                   end::double precision                            as month_price_change,
               valuation.enterprise_value_revenue::double precision as enterprise_value_to_sales,
               highlights.profit_margin::double precision,

               trading_stats.avg_volume_10d,
               trading_stats.short_percent_outstanding,
               trading_stats.shares_outstanding,
               trading_stats.avg_volume_90d,
               trading_stats.shares_float,
               trading_stats.short_ratio,
               trading_stats.beta,
               trading_stats.absolute_historical_volatility_adjusted_current,
               trading_stats.relative_historical_volatility_adjusted_current,
               trading_stats.absolute_historical_volatility_adjusted_min_1y,
               trading_stats.absolute_historical_volatility_adjusted_max_1y,
               trading_stats.relative_historical_volatility_adjusted_min_1y,
               trading_stats.relative_historical_volatility_adjusted_max_1y,
               trading_stats.implied_volatility,

               growth_stats.revenue_growth_yoy,
               growth_stats.revenue_growth_fwd,
               growth_stats.ebitda_growth_yoy,
               growth_stats.eps_growth_yoy,
               growth_stats.eps_growth_fwd
from tickers t
         left join highlights on t.symbol = highlights.symbol
         left join valuation on t.symbol = valuation.symbol
         left join today_price on t.symbol = today_price.code
         left join trading_stats on t.symbol = trading_stats.symbol
         left join growth_stats on t.symbol = growth_stats.symbol