{{
  config(
    materialized = "table",
    post_hook=[
      index(this, 'industry_id'),
      index(this, 'date'),
      fk(this, 'industry_id', this.schema, 'gainy_industries', 'id'),
    ]
  )
}}

select fisq.date::timestamp,
       ti.industry_id,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY net_income)    as median_net_income,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY total_revenue) as median_revenue
from {{ ref('financials_income_statement_quarterly') }} fisq
         join {{ ref('tickers') }} on fisq.symbol = tickers.symbol
         join {{ ref('ticker_industries') }} ti on tickers.symbol = ti.symbol
group by ti.industry_id, fisq.date
