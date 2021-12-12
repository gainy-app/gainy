{{
  config(
    materialized = "table",
    post_hook=[
      'create unique index if not exists {{ get_index_name(this, "profile_id__ticker_symbol") }} (profile_id, ticker_symbol)',
    ]
  )
}}

select profile_holdings_normalized.profile_id,
       portfolio_holding_details.ticker_symbol,
       min(purchase_date)                                      as purchase_date,
       min(portfolio_holding_group_gains.relative_gain_total)  as relative_gain_total,
       min(portfolio_holding_group_gains.relative_gain_1d)     as relative_gain_1d,
       min(portfolio_holding_group_gains.value_to_portfolio_value) as value_to_portfolio_value,
       min(ticker_name)::varchar                               as ticker_name,
       min(market_capitalization)                              as market_capitalization,
       min(next_earnings_date)                                 as next_earnings_date,
       sum(portfolio_holding_details.ltt_quantity_total)       as ltt_quantity_total
from {{ ref('portfolio_holding_details') }}
         join {{ ref('profile_holdings_normalized') }}
              on profile_holdings_normalized.holding_id = portfolio_holding_details.holding_id
         join {{ ref('portfolio_holding_group_gains') }}
              on portfolio_holding_group_gains.profile_id = profile_holdings_normalized.profile_id
                 and portfolio_holding_group_gains.ticker_symbol = portfolio_holding_details.ticker_symbol
group by profile_holdings_normalized.profile_id, portfolio_holding_details.ticker_symbol