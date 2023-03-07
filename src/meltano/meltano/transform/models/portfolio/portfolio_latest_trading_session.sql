{{
  config(
    materialized = "view",
  )
}}


select profile_id, max(week_trading_sessions.date) as date, max(open_at)::timestamp as open_at
from {{ ref('profile_holdings_normalized') }}
         join {{ ref('week_trading_sessions') }}
              on week_trading_sessions.symbol = profile_holdings_normalized.ticker_symbol
where week_trading_sessions.index = 0
group by profile_id
