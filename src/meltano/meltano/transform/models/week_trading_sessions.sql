{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}


select symbol,
       date,
       index,
       case
           when type = 'crypto'
               then now() - ((index + 1) * interval '1 day')
           else open_at
           end as open_at,
       case
           when type = 'crypto'
               then now() - (index * interval '1 day')
           else close_at
           end as close_at
from {{ ref('week_trading_sessions_static') }}
     join {{ ref('base_tickers') }} using (symbol)
