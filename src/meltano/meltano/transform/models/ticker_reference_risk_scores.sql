{{
  config(
    materialized = "table",
    post_hook=[
      pk('symbol'),
      index(['symbol', 'beta'], false),
      index(['symbol', 'volatility_90'], false),
    ]
  )
}}


with filtered_tickers as
         (
             with versioned_data as
                      (
                          select code,
                                 components as holdings
                          from {{ source('eod', 'eod_fundamentals') }}
                          where code in ('RUI.INDX', 'RUT.INDX')
                            and components is not null
                      ),
                  expanded as
                      (
                          select code,
                                 (json_each((holdings)::json)).*
                          from versioned_data
                  )
             select distinct expanded.value ->> 'Code' as symbol
             from expanded
         )
select symbol,
       beta,
       volatility_90,
       percent_rank()
       over (order by beta nulls last rows between unbounded preceding and unbounded following)          as beta_pct,
       percent_rank()
       over (order by volatility_90 nulls last rows between unbounded preceding and unbounded following) as volatility_90_pct
from filtered_tickers
         join {{ ref('ticker_metrics') }} using (symbol)
