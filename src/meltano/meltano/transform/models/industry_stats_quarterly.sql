{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'industry_id'),
      index(this, 'date'),
    ]
  )
}}

select concat(ti.industry_id, '_', fisq.date::timestamp)::varchar as id,
       fisq.date::timestamp,
       ti.industry_id,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY net_income)    as median_net_income,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY total_revenue) as median_revenue
from {{ ref('financials_income_statement_quarterly') }} fisq
         join {{ ref('tickers') }} on fisq.symbol = tickers.symbol
         join {{ ref('ticker_industries') }} ti on tickers.symbol = ti.symbol
group by ti.industry_id, fisq.date
