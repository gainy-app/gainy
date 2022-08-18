{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    post_hook=[
      pk('symbol'),
    ]
  )
}}

select base_tickers.symbol,
       (eod_fundamentals.etf_data -> 'Performance' ->> '1y_Volatility')::double precision as volatility_1y,
       (eod_fundamentals.etf_data -> 'Performance' ->> '3y_Volatility')::double precision as volatility_3y,
       (eod_fundamentals.etf_data ->> 'Dividend_Paying_Frequency')                        as dividend_frequency,
       (eod_fundamentals.etf_data ->> 'NetExpenseRatio')::double precision                as net_expense_ratio,
       (eod_fundamentals.etf_data ->> 'TotalAssets')::bigint                              as total_assets,
       pow(ticker_metrics.price_change_5y + 1, 0.2) - 1                                   as annualized_return_5y

from {{ ref('base_tickers') }}
         left join {{ source('eod', 'eod_fundamentals') }}
                   on eod_fundamentals.code = base_tickers.symbol
         left join {{ ref('ticker_metrics') }} using (symbol)
where base_tickers.type = 'etf'
