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
     raw_eod_fundamentals as (SELECT * FROM {{ source('eod', 'eod_fundamentals') }}),
     marked_prices as
         (
             select distinct on (hp.code, hp.period) *
             from (
                      select *,
                             case
                                 when hp."date" <= hp.cur_date - interval '1 year' then '1y'
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
                    OVER (partition by symbol ORDER BY date desc ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING) as ebitda_ttm
             from financials_income_statement_quarterly
             order by symbol, date desc
         ),
     trading_metrics as
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
                                              case
                                                  when adjusted_close > 0 then
                                                                  stddev_pop(adjusted_close)
                                                                  OVER (partition by code ORDER BY date desc ROWS BETWEEN CURRENT ROW AND 255 FOLLOWING) /
                                                                  adjusted_close
                                                  end                                                                                as relative_historical_volatility_adjusted
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
         select tickers.symbol,
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
         from tickers
                  left join highlights on highlights.symbol = tickers.symbol
                  left join ebitda_growth_yoy on ebitda_growth_yoy.symbol = tickers.symbol
                  left join eps_actual_growth_yoy on eps_actual_growth_yoy.symbol = tickers.symbol
                  left join earnings_trend_0y on earnings_trend_0y.symbol = tickers.symbol
                  left join earnings_trend_1y on earnings_trend_1y.symbol = tickers.symbol
                  left join latest_income_statement_yearly on latest_income_statement_yearly.symbol = tickers.symbol
                  left join latest_earnings_annual on latest_earnings_annual.symbol = tickers.symbol
                  left join latest_expanded_earnings_history_with_eps_actual
                            on latest_expanded_earnings_history_with_eps_actual.symbol = tickers.symbol
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
             select tickers.symbol,
                    highlights.market_capitalization::bigint,
                    valuation.enterprise_value_revenue::double precision as enterprise_value_to_sales,
                    highlights.pe_ratio::double precision                as price_to_earnings_ttm,
                    valuation.price_sales_ttm                            as price_to_sales_ttm,
                    case when highlights.book_value > 0 then
                        today_price.price / highlights.book_value
                        end                                              as price_to_book_value,
                    valuation.enterprise_value_ebidta                    as enterprise_value_to_ebitda
             from tickers
                      left join highlights
                                on tickers.symbol = highlights.symbol
                      left join valuation on tickers.symbol = valuation.symbol
                      left join today_price on tickers.symbol = today_price.code
         ),
     momentum_metrics as
         (
             select symbol,
                    case
                        when today_price.price > 0 then
                                (select mp.price from marked_prices mp where code = t.symbol and period = '1m') /
                                today_price.price
                        end::double precision as price_change_1m,
                    case
                        when today_price.price > 0 then
                                (select mp.price from marked_prices mp where code = t.symbol and period = '3m') /
                                today_price.price
                        end::double precision as price_change_3m,
                    case
                        when today_price.price > 0 then
                                (select mp.price from marked_prices mp where code = t.symbol and period = '1y') /
                                today_price.price
                        end::double precision as price_change_1y
             from tickers t
                      join today_price on t.symbol = today_price.code
         ),
     dividend_metrics as
         (
             with expanded_dividends as
                      (
                          select *,
                                 min(has_grown)
                                 OVER (partition by code ORDER BY date ROWS BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING) as has_grown_sequential
                          from (
                                   select *,
                                          (value >= last_value(value)
                                                    OVER (partition by code ORDER BY date DESC ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING)
                                              and
                                           date::date - last_value(date::date)
                                                        OVER (partition by code ORDER BY date DESC ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) <=
                                           case
                                               when period = 'Annual' then 12 * 30 + 45
                                               when period = 'Quarterly' then 3 * 30 + 45
                                               when period = 'SemiAnnual' then 6 * 30 + 45
                                               when period = 'Monthly' then 30 + 15
                                               end)::int as has_grown
                                   from raw_data.eod_dividends
                                   order by code, raw_data.eod_dividends.date DESC
                               ) t
                          order by code, date DESC
                      ),
                  dividend_stats as
                      (
                          select distinct on (code) code,
                                                    FLOOR(
                                                                        sum(has_grown_sequential)
                                                                        OVER (partition by expanded_dividends.code
                                                                            ORDER BY expanded_dividends.date DESC
                                                                            ROWS BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING
                                                                            ) /
                                                                        4)::int as years_of_consecutive_dividend_growth,
                                                    period::varchar             as dividend_frequency
                          from expanded_dividends
                          order by code, date desc
                      )
             select raw_eod_fundamentals.code                                                  as symbol,
                    highlights.dividend_yield::double precision,
                    highlights.dividend_share::double precision                                as dividends_per_share,
                    (raw_eod_fundamentals.splitsdividends ->> 'PayoutRatio')::double precision as dividend_payout_ratio,
                    dividend_stats.years_of_consecutive_dividend_growth,
                    dividend_stats.dividend_frequency
             from raw_eod_fundamentals
                      left join highlights on raw_eod_fundamentals.code = highlights.symbol
                      left join dividend_stats on raw_eod_fundamentals.code = dividend_stats.code
         ),
     earnings_metrics as
         (
             select tickers.symbol,
                    latest_expanded_earnings_history_with_eps_actual.eps_actual,
                    latest_expanded_earnings_history_with_eps_actual.eps_estimate,
                    highlights.beaten_quarterly_eps_estimation_count_ttm,
                    latest_expanded_earnings_history_with_eps_actual.surprise_percent as eps_surprise,
                    earnings_trend_0y.revenue_estimate_avg                            as revenue_estimate_avg_0y,
                    highlights.revenue_ttm
             from tickers
                      join latest_expanded_earnings_history_with_eps_actual
                           on latest_expanded_earnings_history_with_eps_actual.symbol = tickers.symbol
                      join earnings_trend_0y on earnings_trend_0y.symbol = tickers.symbol
                      join highlights on tickers.symbol = highlights.symbol
         )
select DISTINCT ON
    (t.symbol) t.symbol,

    /* Selected */
               highlights.profit_margin::double precision,

               trading_metrics.avg_volume_10d,
               trading_metrics.short_percent_outstanding,
               trading_metrics.shares_outstanding,
               trading_metrics.avg_volume_90d,
               trading_metrics.shares_float,
               trading_metrics.short_ratio,
               trading_metrics.beta,
               trading_metrics.absolute_historical_volatility_adjusted_current,
               trading_metrics.relative_historical_volatility_adjusted_current,
               trading_metrics.absolute_historical_volatility_adjusted_min_1y,
               trading_metrics.absolute_historical_volatility_adjusted_max_1y,
               trading_metrics.relative_historical_volatility_adjusted_min_1y,
               trading_metrics.relative_historical_volatility_adjusted_max_1y,
               trading_metrics.implied_volatility,

               growth_metrics.revenue_growth_yoy,
               growth_metrics.revenue_growth_fwd,
               growth_metrics.ebitda_growth_yoy,
               growth_metrics.eps_growth_yoy,
               growth_metrics.eps_growth_fwd,

               general_data.address_city,
               general_data.address_state,
               general_data.address_county,
               general_data.address_full,
               general_data.exchange_name,

               valuation_metrics.market_capitalization,
               valuation_metrics.enterprise_value_to_sales,
               valuation_metrics.price_to_earnings_ttm,
               valuation_metrics.price_to_sales_ttm,
               valuation_metrics.price_to_book_value,
               valuation_metrics.enterprise_value_to_ebitda,

               momentum_metrics.price_change_1m,
               momentum_metrics.price_change_3m,
               momentum_metrics.price_change_1y,

               dividend_metrics.dividend_yield,
               dividend_metrics.dividends_per_share,
               dividend_metrics.dividend_payout_ratio,
               dividend_metrics.years_of_consecutive_dividend_growth,
               dividend_metrics.dividend_frequency,

               earnings_metrics.eps_actual,
               earnings_metrics.eps_estimate,
               earnings_metrics.beaten_quarterly_eps_estimation_count_ttm,
               earnings_metrics.eps_surprise,
               earnings_metrics.revenue_estimate_avg_0y,
               earnings_metrics.revenue_ttm
from tickers t
         left join highlights on t.symbol = highlights.symbol
         left join today_price on t.symbol = today_price.code
         left join trading_metrics on t.symbol = trading_metrics.symbol
         left join growth_metrics on t.symbol = growth_metrics.symbol
         left join general_data on t.symbol = general_data.symbol
         left join valuation_metrics on t.symbol = valuation_metrics.symbol
         left join momentum_metrics on t.symbol = momentum_metrics.symbol
         left join dividend_metrics on t.symbol = dividend_metrics.symbol
         left join earnings_metrics on t.symbol = earnings_metrics.symbol