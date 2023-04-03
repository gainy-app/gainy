{{
  config(
    materialized = "view",
  )
}}


select distinct on (
    symbol
    ) *
from {{ ref('ticker_realtime_metrics_raw') }}
order by symbol desc, updated_at desc