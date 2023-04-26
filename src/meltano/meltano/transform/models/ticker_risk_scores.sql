{{
  config(
    materialized = "table",
    post_hook=[
      pk('symbol'),
    ]
  )
}}


with data as
         (
             select symbol,
                    coalesce(public.last_value_ignorenulls(beta_pct)
                             over (order by ticker_metrics.beta nulls last rows between unbounded preceding and current row),
                             0) as beta_pct,
                    coalesce(public.last_value_ignorenulls(volatility_90_pct)
                             over (order by ticker_metrics.volatility_90 nulls last rows between unbounded preceding and current row),
                             0) as volatility_90_pct
             from {{ ref('ticker_metrics') }}
                      left join {{ ref('ticker_reference_risk_scores') }} using (symbol)
     )
select symbol,
       0.75 * volatility_90_pct + 0.25 * beta_pct as risk_score
from data
