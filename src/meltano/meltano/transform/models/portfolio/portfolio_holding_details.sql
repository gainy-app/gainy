{{
  config(
    materialized = "table",
    unique_key = "holding_id_v2",
    tags = ["realtime"],
    post_hook=[
      pk('holding_id_v2'),
      index('ticker_symbol'),
    ]
  )
}}

select profile_holdings_normalized_all.holding_id_v2,
       profile_holdings_normalized_all.holding_id,
       profile_holdings_normalized_all.symbol           as ticker_symbol,
       holding_since as purchase_date,
       coalesce(ticker_options.name, base_tickers.name) as ticker_name
from {{ ref('profile_holdings_normalized_all') }}
         left join {{ ref('base_tickers') }} on base_tickers.symbol = profile_holdings_normalized_all.ticker_symbol
         left join {{ ref('ticker_options') }} on ticker_options.contract_name = profile_holdings_normalized_all.symbol
where not profile_holdings_normalized_all.is_hidden
