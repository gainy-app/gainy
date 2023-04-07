{{
  config(
    materialized = "incremental",
    post_hook=[
      index(['symbol', 'updated_at'], false),
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}

with highlights as (select * from {{ ref('highlights') }}),
     base_tickers as (select symbol from {{ ref('base_tickers') }}),
     all_tickers as
         (
             select symbol from {{ ref('base_tickers') }}
             union all
             select contract_name as symbol from {{ ref('ticker_options_monitored') }}
         ),
     valuation as (select * from {{ ref('valuation') }}),
     technicals as (select * from {{ ref('technicals') }}),
     ticker_shares_stats as (select * from {{ ref('ticker_shares_stats') }}),
     financials_income_statement_quarterly as (select * from {{ ref('financials_income_statement_quarterly') }}),
     financials_income_statement_yearly as (select * from {{ ref('financials_income_statement_yearly') }}),
     financials_balance_sheet_quarterly as (select * from {{ ref('financials_balance_sheet_quarterly') }}),
     earnings_trend as (select * from {{ ref('earnings_trend') }}),
     earnings_history as (select * from {{ ref('earnings_history') }}),
     earnings_annual as (select * from {{ ref('earnings_annual') }}),
     historical_prices as (select * from {{ ref('historical_prices') }}),
     historical_prices_marked as (select * from {{ ref('historical_prices_marked') }}),
     raw_eod_options as (SELECT * FROM {{ source('eod', 'eod_options') }}),
     raw_eod_fundamentals as (SELECT * FROM {{ source('eod', 'eod_fundamentals') }}),
     expanded_earnings_history as
         (
             select *,
                    sum(eps_actual)
                    OVER (partition by symbol ORDER BY date desc ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING) as eps_actual_ttm
             from earnings_history
             order by symbol, date desc
         ),

     latest_expanded_earnings_history_with_eps_actual as
         (
             select distinct on (symbol) *
             from expanded_earnings_history
             where eps_actual is not null
             order by symbol, date desc
         ),
     earnings_trend_0y as
         (
             select distinct on (symbol) * from earnings_trend where period = '0y' order by symbol, date desc
         ),
     latest_income_statement_yearly as
         (
             select distinct on (symbol) * from financials_income_statement_yearly order by symbol, date desc
         ),
     expanded_income_statement_quarterly as
         (
             select *,
                    sum(ebitda)
                    OVER (partition by symbol ORDER BY date desc ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING) as ebitda_ttm,
                    sum(gross_profit)
                    OVER (partition by symbol ORDER BY date desc ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING) as gross_profit_ttm,
                    sum(cost_of_revenue)
                    OVER (partition by symbol ORDER BY date desc ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING) as cost_of_revenue_ttm,
                    sum(net_income)
                    OVER (partition by symbol ORDER BY date desc ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING) as net_income_ttm
             from financials_income_statement_quarterly
             order by symbol, date desc
         ),
     trading_metrics as
         (
             with avg_volume_10d as
                      (
                          select symbol,
                                 avg(volume * adjusted_close) as value_money,
                                 avg(hp.volume) as value
                          from historical_prices hp
                          where hp.date > now() - interval '10 days'
                          group by symbol
                      ),
                  avg_volume_90d as
                      (
                          select symbol,
                                 avg(volume * adjusted_close) as value_money,
                                 avg(hp.volume) as value
                          from historical_prices hp
                          where hp.date > now() - interval '90 days'
                          group by symbol
                      ),
                  historical_volatility as
                      (
                          with volatility as
                                   (
                                       select symbol,
                                              date,
                                              stddev_pop(adjusted_close)
                                              OVER (partition by symbol ORDER BY date desc ROWS BETWEEN CURRENT ROW AND 255 FOLLOWING) as absolute_historical_volatility_adjusted,
                                              case
                                                  when adjusted_close > 0 then
                                                                  stddev_pop(adjusted_close)
                                                                  OVER (partition by symbol ORDER BY date desc ROWS BETWEEN CURRENT ROW AND 255 FOLLOWING) /
                                                                  adjusted_close
                                                  end                                                                                as relative_historical_volatility_adjusted
                                       from historical_prices
                                       where date > now() - interval '2 year'
                                       order by symbol, date desc
                                   ),
                               volatility_current as
                                   (
                                       select distinct on (symbol) symbol,
                                                                 volatility.absolute_historical_volatility_adjusted as absolute_historical_volatility_adjusted_current,
                                                                 volatility.relative_historical_volatility_adjusted as relative_historical_volatility_adjusted_current
                                       from volatility
                                       where date > now() - interval '1 year'
                                       order by symbol, date desc
                                   ),
                               volatility_stats as
                                   (
                                       select symbol,
                                              min(volatility.absolute_historical_volatility_adjusted) as absolute_historical_volatility_adjusted_min_1y,
                                              max(volatility.absolute_historical_volatility_adjusted) as absolute_historical_volatility_adjusted_max_1y,
                                              min(volatility.relative_historical_volatility_adjusted) as relative_historical_volatility_adjusted_min_1y,
                                              max(volatility.relative_historical_volatility_adjusted) as relative_historical_volatility_adjusted_max_1y
                                       from volatility
                                       where date > now() - interval '1 year'
                                       group by volatility.symbol
                                   )
                          select volatility_current.*,
                                 volatility_stats.absolute_historical_volatility_adjusted_min_1y,
                                 volatility_stats.absolute_historical_volatility_adjusted_max_1y,
                                 volatility_stats.relative_historical_volatility_adjusted_min_1y,
                                 volatility_stats.relative_historical_volatility_adjusted_max_1y
                          from volatility_current
                                   join volatility_stats using (symbol)
                      ),
                  implied_volatility as
                      (
                          select code as symbol,
                                 avg(impliedvolatility) as value
                          from raw_eod_options
                          group by code
                      )
             select t.symbol,
                    avg_volume_10d.value_money::double precision as avg_volume_10d_money,
                    avg_volume_10d.value::double precision       as avg_volume_10d,
                    ticker_shares_stats.shares_outstanding::bigint,
                    avg_volume_90d.value_money::double precision as avg_volume_90d_money,
                    avg_volume_90d.value::double precision       as avg_volume_90d,
                    ticker_shares_stats.shares_float::bigint,
                    technicals.short_ratio::double precision,
                    technicals.short_percent::double precision,
                    technicals.beta::double precision,
                    historical_volatility.absolute_historical_volatility_adjusted_current,
                    historical_volatility.relative_historical_volatility_adjusted_current,
                    historical_volatility.absolute_historical_volatility_adjusted_min_1y,
                    historical_volatility.absolute_historical_volatility_adjusted_max_1y,
                    historical_volatility.relative_historical_volatility_adjusted_min_1y,
                    historical_volatility.relative_historical_volatility_adjusted_max_1y,
                    implied_volatility.value                     as implied_volatility
             from base_tickers t
                      left join ticker_shares_stats using (symbol)
                      left join technicals using (symbol)
                      left join avg_volume_10d using (symbol)
                      left join avg_volume_90d using (symbol)
                      left join historical_volatility using (symbol)
                      left join implied_volatility using (symbol)
         ),
     growth_metrics as (
         with ebitda_growth_yoy as
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
              earnings_trend_1y as
                  (
                      select distinct on (symbol) * from earnings_trend where period = '+1y' order by symbol, date desc
                  ),
              latest_earnings_annual as
                  (
                      select distinct on (symbol) *
                      from earnings_annual
                      order by symbol, date desc
                  )
         select base_tickers.symbol,
                highlights.quarterly_revenue_growth_yoy::double precision        as revenue_growth_yoy,
                -- SeekingAlpha: The forward growth rate is a compounded annual growth rate from the most recently completed fiscal year's revenue (FY (-1)) to analysts' consensus revenue estimates for two fiscal years forward (FY 2).
                case
                    when latest_income_statement_yearly.total_revenue > 0 then
                        coalesce(sqrt(case
                                          when earnings_trend_1y.revenue_estimate_avg > 0
                                              then earnings_trend_1y.revenue_estimate_avg end /
                                      latest_income_statement_yearly.total_revenue) - 1,
                                 case
                                     when earnings_trend_0y.revenue_estimate_avg > 0
                                         then earnings_trend_0y.revenue_estimate_avg end /
                                 latest_income_statement_yearly.total_revenue - 1)
                    end                                                          as revenue_growth_fwd,
                ebitda_growth_yoy.value                                          as ebitda_growth_yoy,
                eps_actual_growth_yoy.value                                      as eps_growth_yoy,
                -- SeekingAlpha: The forward growth rate is a compounded annual growth rate from the most recently completed fiscal year's EPS (FY(-1)) to analysts' consensus EPS estimates for two fiscal years forward (FY2).
                case
                    when latest_earnings_annual.eps_actual > 0 then
                        coalesce(sqrt(case
                                          when earnings_trend_1y.earnings_estimate_avg > 0
                                              then earnings_trend_1y.earnings_estimate_avg end /
                                      latest_earnings_annual.eps_actual) - 1,
                                 case
                                     when earnings_trend_0y.earnings_estimate_avg > 0
                                         then earnings_trend_0y.earnings_estimate_avg end /
                                 latest_earnings_annual.eps_actual - 1)
                    end                                                          as eps_growth_fwd
         from base_tickers
                  left join highlights on highlights.symbol = base_tickers.symbol
                  left join ebitda_growth_yoy on ebitda_growth_yoy.symbol = base_tickers.symbol
                  left join eps_actual_growth_yoy on eps_actual_growth_yoy.symbol = base_tickers.symbol
                  left join earnings_trend_0y on earnings_trend_0y.symbol = base_tickers.symbol
                  left join earnings_trend_1y on earnings_trend_1y.symbol = base_tickers.symbol
                  left join latest_income_statement_yearly on latest_income_statement_yearly.symbol = base_tickers.symbol
                  left join latest_earnings_annual on latest_earnings_annual.symbol = base_tickers.symbol
                  left join latest_expanded_earnings_history_with_eps_actual
                            on latest_expanded_earnings_history_with_eps_actual.symbol = base_tickers.symbol
     ),
     general_data as
         (
             select code                                                          as symbol,
                    (general -> 'AddressData' ->> 'City') :: character varying    as address_city,
                    (general -> 'AddressData' ->> 'State') :: character varying   as address_state,
                    (general -> 'AddressData' ->> 'Country') :: character varying as address_county,
                    (general ->> 'Address') :: character varying                  as address_full,
                    (general ->> 'Exchange') :: character varying                 as exchange_name
             from raw_eod_fundamentals
         ),
     valuation_metrics as
         (
             select base_tickers.symbol,
                    highlights.market_capitalization::bigint,
                    valuation.enterprise_value_revenue::double precision as enterprise_value_to_sales,
                    highlights.pe_ratio::double precision                as price_to_earnings_ttm,
                    valuation.price_sales_ttm                            as price_to_sales_ttm,
                    case when highlights.book_value > 0 then
                                 historical_prices_marked.price_0d / highlights.book_value
                        end                                              as price_to_book_value,
                    valuation.enterprise_value_ebidta                    as enterprise_value_to_ebitda
             from base_tickers
                      left join highlights
                                on base_tickers.symbol = highlights.symbol
                      left join valuation on base_tickers.symbol = valuation.symbol
                      left join historical_prices_marked on base_tickers.symbol = historical_prices_marked.symbol
         ),
     momentum_metrics as
         (
             with weekly_prices as
                      (
                          SELECT symbol, datetime, adjusted_close
                          from {{ ref('historical_prices_aggregated_1w') }}
                          where datetime > NOW() - interval '3 years'
                      ),
                  weekly_prices2 as
                      (
                          select *,
                                 first_value(adjusted_close) over (partition by symbol order by datetime rows between 1 preceding and 0 preceding) as prev_week_adjusted_close
                          from weekly_prices
                      )
             SELECT symbol,
                    stddev_pop(adjusted_close / prev_week_adjusted_close - 1) * pow(52, 0.5) as stddev_3_years
             from weekly_prices2
             where prev_week_adjusted_close > 0
             group by symbol
         ),
     dividend_metrics as
         (
             with expanded_dividends as
                      (
                          select *,
                                 min(has_grown)
                                 OVER (partition by symbol ORDER BY date ROWS BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING) as has_grown_sequential
                          from (
                                   select *,
                                          (value >= last_value(value)
                                                    OVER (partition by symbol ORDER BY date DESC ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING)
                                              and
                                           date - last_value(date)
                                                  OVER (partition by symbol ORDER BY date DESC ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) <=
                                           case
                                               when period = 'Annual' then 12 * 30 + 45
                                               when period = 'Quarterly' then 3 * 30 + 45
                                               when period = 'SemiAnnual' then 6 * 30 + 45
                                               when period = 'Monthly' then 30 + 15
                                               end)::int as has_grown
                                   from {{ ref('historical_dividends') }}
                                   order by symbol, date DESC
                               ) t
                          order by symbol, date DESC
                      ),
                  dividend_stats as
                      (
                          select distinct on (
                              symbol
                              ) symbol,
                                FLOOR(sum(has_grown_sequential)
                                      OVER (partition by symbol ORDER BY date DESC ROWS BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING)
                                      / 4)::int as years_of_consecutive_dividend_growth,
                                period as dividend_frequency
                          from expanded_dividends
                          order by symbol, date desc
                      )
             select raw_eod_fundamentals.code                                                  as symbol,
                    highlights.dividend_yield::double precision,
                    highlights.dividend_share::double precision                                as dividends_per_share,
                    (raw_eod_fundamentals.splitsdividends ->> 'PayoutRatio')::double precision as dividend_payout_ratio,
                    dividend_stats.years_of_consecutive_dividend_growth,
                    dividend_stats.dividend_frequency
             from raw_eod_fundamentals
                      left join highlights on raw_eod_fundamentals.code = highlights.symbol
                      left join dividend_stats on raw_eod_fundamentals.code = dividend_stats.symbol
         ),
     earnings_metrics as
         (
             select base_tickers.symbol,
                    latest_expanded_earnings_history_with_eps_actual.eps_actual_ttm::double precision,
                    latest_expanded_earnings_history_with_eps_actual.eps_actual::double precision,
                    latest_expanded_earnings_history_with_eps_actual.eps_estimate,
                    highlights.beaten_quarterly_eps_estimation_count_ttm,
                    latest_expanded_earnings_history_with_eps_actual.surprise_percent as eps_surprise,
                    latest_expanded_earnings_history_with_eps_actual.eps_difference,
                    earnings_trend_0y.revenue_estimate_avg                            as revenue_estimate_avg_0y,
                    highlights.revenue_ttm::double precision
             from base_tickers
                      join latest_expanded_earnings_history_with_eps_actual
                           on latest_expanded_earnings_history_with_eps_actual.symbol = base_tickers.symbol
                      join earnings_trend_0y on earnings_trend_0y.symbol = base_tickers.symbol
                      join highlights on base_tickers.symbol = highlights.symbol
         ),
     financials_metrics as
         (
             with latest_balance_sheet_quarterly as
                      (
                          select distinct on (symbol) * from financials_balance_sheet_quarterly order by symbol, date desc
                      )
             select base_tickers.symbol,
                    highlights.revenue_per_share_ttm::double precision,
                    latest_income_statement_yearly.net_income,
                    expanded_income_statement_quarterly.net_income_ttm,
                    case
                        when expanded_income_statement_quarterly.cost_of_revenue_ttm > 0
                            then expanded_income_statement_quarterly.gross_profit_ttm /
                                 expanded_income_statement_quarterly.cost_of_revenue_ttm
                        end                             as roi,
                    latest_balance_sheet_quarterly.cash as asset_cash_and_equivalents,
                    case
                        when latest_balance_sheet_quarterly.total_assets > 0
                            then expanded_income_statement_quarterly.net_income_ttm / latest_balance_sheet_quarterly.total_assets
                        end                             as roa,
                    latest_balance_sheet_quarterly.total_assets,
                    latest_income_statement_yearly.ebitda,
                    expanded_income_statement_quarterly.ebitda_ttm,
                    latest_balance_sheet_quarterly.net_debt
             from base_tickers
                      join highlights on base_tickers.symbol = highlights.symbol
                      join latest_income_statement_yearly on latest_income_statement_yearly.symbol = base_tickers.symbol
                      join latest_balance_sheet_quarterly on latest_balance_sheet_quarterly.symbol = base_tickers.symbol
                      join expanded_income_statement_quarterly on expanded_income_statement_quarterly.symbol = base_tickers.symbol
         ),
     next_earnings_date as
         (
             select symbol,
                    min(report_date) as date
             from {{ ref('earnings_history') }}
             where report_date >= now()
             group by symbol
         )
select DISTINCT ON
    (t.symbol) t.symbol,

    /* Selected */
               highlights.profit_margin::double precision,

               trading_metrics.avg_volume_10d,
               trading_metrics.avg_volume_10d_money,
               trading_metrics.shares_outstanding,
               trading_metrics.avg_volume_90d,
               trading_metrics.avg_volume_90d_money,
               trading_metrics.shares_float,
               trading_metrics.short_ratio,
               trading_metrics.short_percent,
               trading_metrics.short_percent as short_percent_outstanding, -- todo remove
               trading_metrics.beta,
               trading_metrics.absolute_historical_volatility_adjusted_current,
               trading_metrics.relative_historical_volatility_adjusted_current,
               trading_metrics.absolute_historical_volatility_adjusted_min_1y,
               trading_metrics.absolute_historical_volatility_adjusted_max_1y,
               trading_metrics.relative_historical_volatility_adjusted_min_1y,
               trading_metrics.relative_historical_volatility_adjusted_max_1y,
               trading_metrics.implied_volatility,

               growth_metrics.revenue_growth_yoy,
               growth_metrics.revenue_growth_fwd::double precision,
               growth_metrics.ebitda_growth_yoy::double precision,
               growth_metrics.eps_growth_yoy::double precision,
               growth_metrics.eps_growth_fwd::double precision,

               general_data.address_city,
               general_data.address_state,
               general_data.address_county,
               general_data.address_full,
               general_data.exchange_name,

               valuation_metrics.market_capitalization,
               valuation_metrics.enterprise_value_to_sales,
               valuation_metrics.price_to_earnings_ttm,
               valuation_metrics.price_to_sales_ttm::double precision,
               valuation_metrics.price_to_book_value,
               valuation_metrics.enterprise_value_to_ebitda::double precision,

               momentum_metrics.stddev_3_years,

               dividend_metrics.dividend_yield,
               dividend_metrics.dividends_per_share,
               dividend_metrics.dividend_payout_ratio,
               dividend_metrics.years_of_consecutive_dividend_growth,
               dividend_metrics.dividend_frequency,

               earnings_metrics.eps_actual_ttm as eps_ttm,
               earnings_metrics.eps_actual,
               earnings_metrics.eps_estimate::double precision,
               earnings_metrics.beaten_quarterly_eps_estimation_count_ttm,
               earnings_metrics.eps_surprise::double precision,
               earnings_metrics.eps_difference::double precision,
               earnings_metrics.revenue_estimate_avg_0y::double precision,
               earnings_metrics.revenue_ttm as revenue_actual,

               earnings_metrics.revenue_ttm,
               financials_metrics.revenue_per_share_ttm,
               financials_metrics.net_income::double precision,
               financials_metrics.net_income_ttm::double precision,
               financials_metrics.roi::double precision,
               financials_metrics.asset_cash_and_equivalents::double precision,
               financials_metrics.roa::double precision,
               financials_metrics.total_assets::double precision,
               financials_metrics.ebitda::double precision,
               financials_metrics.ebitda_ttm::double precision,
               financials_metrics.net_debt::double precision,

               next_earnings_date.date::date as next_earnings_date,

               now() as updated_at
from all_tickers t
         left join highlights on t.symbol = highlights.symbol
         left join trading_metrics on t.symbol = trading_metrics.symbol
         left join growth_metrics on t.symbol = growth_metrics.symbol
         left join general_data on t.symbol = general_data.symbol
         left join valuation_metrics on t.symbol = valuation_metrics.symbol
         left join momentum_metrics on t.symbol = momentum_metrics.symbol
         left join dividend_metrics on t.symbol = dividend_metrics.symbol
         left join earnings_metrics on t.symbol = earnings_metrics.symbol
         left join financials_metrics on t.symbol = financials_metrics.symbol
         left join next_earnings_date on t.symbol = next_earnings_date.symbol