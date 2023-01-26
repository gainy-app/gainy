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

with next_earnings_date as
         (
             select symbol,
                    min(report_date) as date
             from {{ ref('earnings_history') }}
             where report_date >= now()
             group by symbol
         )
select profile_holdings_normalized_all.holding_id_v2,
       profile_holdings_normalized_all.holding_id,
       profile_holdings_normalized_all.symbol         as ticker_symbol,   -- deprecated
       profile_holdings_normalized_all.account_id,                        -- deprecated
       holding_since                              as purchase_date,
       relative_gain_total,                                               -- deprecated
       relative_gain_1d,                                                  -- deprecated
       portfolio_holding_gains.value_to_portfolio_value,                  -- deprecated
       coalesce(ticker_options.name, base_tickers.name,
                profile_holdings_normalized_all.name) as ticker_name,     -- deprecated
       ticker_metrics.market_capitalization,                              -- deprecated
       next_earnings_date.date::timestamp         as next_earnings_date,  -- deprecated
       profile_holdings_normalized_all.type           as security_type,   -- deprecated
       portfolio_holding_gains.ltt_quantity_total,                        -- deprecated
       profile_holdings_normalized_all.name,                              -- deprecated
       profile_holdings_normalized_all.quantity,                          -- deprecated
       case
           when quantity > 0
               then (actual_value - absolute_gain_total) / quantity
           end                                    as avg_cost -- deprecated
from {{ ref('profile_holdings_normalized_all') }}
         left join {{ ref('portfolio_holding_gains') }} using (holding_id_v2)
         left join {{ ref('base_tickers') }} on base_tickers.symbol = profile_holdings_normalized_all.ticker_symbol
         left join next_earnings_date on next_earnings_date.symbol = base_tickers.symbol
         left join {{ ref('ticker_metrics') }} on ticker_metrics.symbol = base_tickers.symbol
         left join {{ ref('ticker_options') }} on ticker_options.contract_name = profile_holdings_normalized_all.symbol
where not profile_holdings_normalized_all.is_hidden
