{{
  config(
    materialized = "view",
  )
}}

select symbol, date, open_at::timestamp
from {{ ref('week_trading_sessions_static') }}
where index = 0
