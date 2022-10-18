{{
  config(
    materialized = "incremental",
    unique_key = "holding_id",
    tags = ["realtime"],
    post_hook=[
      index('holding_id', true),
      index('ticker_symbol'),
    ]
  )
}}

with first_purchase_date as
         (
             select distinct on (
                 profile_holdings.id
                 ) profile_holdings.id as plaid_holding_id,
                   date
             from {{ source('app', 'profile_portfolio_transactions') }}
                      join {{ source('app', 'profile_holdings') }}
                           on profile_holdings.profile_id = profile_portfolio_transactions.profile_id
                               and profile_holdings.security_id = profile_portfolio_transactions.security_id
                               and profile_holdings.account_id = profile_portfolio_transactions.account_id
             order by profile_holdings.id, date
         ),
     next_earnings_date as
         (
             select symbol,
                    min(report_date) as date
             from {{ ref('earnings_history') }}
             where report_date >= now()
             group by symbol
         )
select profile_holdings_normalized.holding_id,
       portfolio_securities_normalized.original_ticker_symbol as ticker_symbol,
       profile_holdings_normalized.account_id,
       first_purchase_date.date::timestamp                    as purchase_date,
       relative_gain_total,
       relative_gain_1d,
       portfolio_holding_gains.value_to_portfolio_value,
       coalesce(ticker_options.name, base_tickers.name,
                portfolio_securities_normalized.name)         as ticker_name,
       ticker_metrics.market_capitalization,
       next_earnings_date.date::timestamp                     as next_earnings_date,
       portfolio_securities_normalized.type                   as security_type,
       portfolio_holding_gains.ltt_quantity_total,
       profile_holdings_normalized.name,
       profile_holdings_normalized.quantity,
       (actual_value - absolute_gain_total) / quantity        as avg_cost
from {{ ref('profile_holdings_normalized') }}
         left join first_purchase_date using (plaid_holding_id)
         left join {{ ref('portfolio_holding_gains') }} on portfolio_holding_gains.holding_id = profile_holdings_normalized.holding_id
         left join {{ ref('portfolio_securities_normalized') }} on portfolio_securities_normalized.id = profile_holdings_normalized.security_id
         left join {{ ref('base_tickers') }} on base_tickers.symbol = portfolio_securities_normalized.ticker_symbol
         left join next_earnings_date on next_earnings_date.symbol = base_tickers.symbol
         left join {{ ref('ticker_metrics') }} on ticker_metrics.symbol = base_tickers.symbol
         left join {{ ref('ticker_options') }} on ticker_options.contract_name = portfolio_securities_normalized.original_ticker_symbol
