{{
  config(
    materialized = "table",
    post_hook=[
      pk('symbol'),
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
         ),
     volatility_90 as
         (
             select symbol,
                    stddev(relative_daily_gain) * pow(252, 0.5) as volatility_90
             from {{ ref('historical_prices') }}
             where date >= now() - interval '90 days'
             group by symbol
     ),
     data_skeleton as
         (
             select symbol,
                    beta,
                    volatility_90,
                    percent_rank()
                    over (order by beta nulls last rows between unbounded preceding and unbounded following)          as beta_pct,
                    percent_rank()
                    over (order by volatility_90 nulls last rows between unbounded preceding and unbounded following) as volatility_90_pct
             from filtered_tickers
                      join {{ ref('ticker_metrics') }} using (symbol)
                      join volatility_90 using (symbol)
     ),
     data as
         (
             select symbol,
                    ticker_metrics.beta,
                    volatility_90.volatility_90,
                    coalesce(public.last_value_ignorenulls(beta_pct)
                             over (order by ticker_metrics.beta nulls last rows between unbounded preceding and current row),
                             0) as beta_pct,
                    coalesce(public.last_value_ignorenulls(volatility_90_pct)
                             over (order by volatility_90.volatility_90 nulls last rows between unbounded preceding and current row),
                             0) as volatility_90_pct
             from {{ ref('ticker_metrics') }}
                      join volatility_90 using (symbol)
                      left join data_skeleton using (symbol)
     )
select symbol,
       0.75 * volatility_90_pct + 0.25 * beta_pct as risk_score
from data
