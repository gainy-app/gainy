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

with first_purchase_date as
         (
             select holding_id_v2,
                    min(datetime) as datetime
             from {{ ref('portfolio_expanded_transactions') }}
             group by holding_id_v2
         ),
     next_earnings_date as
         (
             select symbol,
                    min(report_date) as date
             from {{ ref('earnings_history') }}
             where report_date >= now()
             group by symbol
         )
select profile_holdings_normalized.holding_id_v2,
       profile_holdings_normalized.holding_id,
       profile_holdings_normalized.symbol         as ticker_symbol,
       profile_holdings_normalized.account_id,
       first_purchase_date.datetime::timestamp    as purchase_date,
       relative_gain_total,
       relative_gain_1d,
       portfolio_holding_gains.value_to_portfolio_value,
       coalesce(ticker_options.name, base_tickers.name,
                profile_holdings_normalized.name) as ticker_name,
       ticker_metrics.market_capitalization,
       next_earnings_date.date::timestamp         as next_earnings_date,
       profile_holdings_normalized.type           as security_type,
       portfolio_holding_gains.ltt_quantity_total,
       profile_holdings_normalized.name,
       profile_holdings_normalized.quantity,
       case
           when quantity > 0
               then (actual_value - absolute_gain_total) / quantity
           end                                    as avg_cost
from {{ ref('profile_holdings_normalized') }}
         left join first_purchase_date using (holding_id_v2)
         left join {{ ref('portfolio_holding_gains') }} using (holding_id_v2)
         left join {{ ref('base_tickers') }} on base_tickers.symbol = profile_holdings_normalized.ticker_symbol
         left join next_earnings_date on next_earnings_date.symbol = base_tickers.symbol
         left join {{ ref('ticker_metrics') }} on ticker_metrics.symbol = base_tickers.symbol
         left join {{ ref('ticker_options') }} on ticker_options.contract_name = profile_holdings_normalized.symbol
