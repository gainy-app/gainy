{{
  config(
    materialized = "table",
    post_hook=[
      pk('collection_uniq_id'),
    ]
  )
}}


with data as
         (
             select collection_uniq_id,
                    collection_id,
                    coalesce(public.last_value_ignorenulls(beta_pct)
                             over (order by beta nulls last rows between unbounded preceding and current row),
                             0) as beta_pct,
                    coalesce(public.last_value_ignorenulls(volatility_90_pct)
                             over (order by volatility_90 nulls last rows between unbounded preceding and current row),
                             0) as volatility_90_pct
             from (
                      select collection_uniq_id,
                             collection_id,
                             beta,
                             null::double precision as beta_pct,
                             volatility_90,
                             null::double precision as volatility_90_pct
                      from {{ ref('collection_metrics') }}

                      union all

                      select null as collection_uniq_id,
                             null as collection_id,
                             beta,
                             beta_pct,
                             volatility_90,
                             volatility_90_pct
                      from {{ ref('ticker_reference_risk_scores') }}
                  ) t
     )
select collection_uniq_id,
       collection_id,
       0.75 * volatility_90_pct + 0.25 * beta_pct as risk_score
from data
where collection_uniq_id is not null
