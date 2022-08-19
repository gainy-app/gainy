{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    post_hook=[
      pk('symbol'),
    ]
  )
}}


with annual_close_prices as
         (
             select distinct on (
                 symbol,
                 date_trunc('year', date)
                 ) symbol,
                   date,
                   adjusted_close
             from {{ ref('base_tickers') }}
                      join {{ ref('historical_prices') }} using (symbol)
             where base_tickers.type = 'etf'
               and adjusted_close > 0
             order by symbol, date_trunc('year', date), date desc
         ),
     annual_returns as
         (
             select symbol,
                    date,
                    adjusted_close / lag(adjusted_close) over (partition by symbol order by date) - 1 as annual_return
             from annual_close_prices
         ),
     return_stats as
         (
             select symbol,
                    min(annual_return) as min_annual_return,
                    max(annual_return) as max_annual_return
             from annual_returns
             group by symbol
         )
select base_tickers.symbol,
       (eod_fundamentals.etf_data -> 'Performance' ->> '1y_Volatility')::double precision as volatility_1y,
       (eod_fundamentals.etf_data -> 'Performance' ->> '3y_Volatility')::double precision as volatility_3y,
       (eod_fundamentals.etf_data ->> 'Dividend_Paying_Frequency')                        as dividend_frequency,
       (eod_fundamentals.etf_data ->> 'NetExpenseRatio')::double precision                as net_expense_ratio,
       floor((eod_fundamentals.etf_data ->> 'TotalAssets')::numeric)::bigint              as total_assets,

       pow(ticker_metrics.price_change_5y + 1, 0.2) - 1                                   as annualized_return_5y,
       return_stats.min_annual_return,
       return_stats.max_annual_return

from {{ ref('base_tickers') }}
         left join {{ source('eod', 'eod_fundamentals') }}
                   on eod_fundamentals.code = base_tickers.symbol
         left join {{ ref('ticker_metrics') }} using (symbol)
         left join return_stats using (symbol)
where base_tickers.type = 'etf'