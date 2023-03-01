{{
  config(
    materialized = "view",
  )
}}

select collection_uniq_id, max(week_trading_sessions_static.date) as date, max(open_at)::timestamp as open_at
from {{ ref('collection_ticker_actual_weights') }}
         join {{ ref('week_trading_sessions_static') }} using (symbol)
where index = 0
group by collection_uniq_id