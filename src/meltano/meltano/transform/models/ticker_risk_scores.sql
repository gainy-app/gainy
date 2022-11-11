{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    post_hook=[
      pk('symbol'),
    ]
  )
}}


with volatility_90 as
         (
             select symbol,
                    stddev(relative_daily_gain) * pow(252, 0.5) as volatility_90
             from historical_prices
             where date >= now() - interval '90 days'
             group by symbol
         )
select symbol,
       0.75 * volatility_90_pct + 0.25 * beta_pct as risk_score
from (
         select symbol,
                beta,
                volatility_90,
                percent_rank()
                over (order by beta rows between unbounded preceding and unbounded following)          as beta_pct,
                percent_rank()
                over (order by volatility_90 rows between unbounded preceding and unbounded following) as volatility_90_pct
         from {{ ref('ticker_metrics') }}
                  join {{ source('app', 'drivewealth_instruments') }} using (symbol)
                  join volatility_90 using (symbol)
     ) t