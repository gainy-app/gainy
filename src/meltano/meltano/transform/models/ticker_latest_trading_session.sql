{{
  config(
    materialized = "view",
  )
}}

select symbol, date, open_at::timestamp
from {{ ref('week_trading_sessions') }}
where index = 0
