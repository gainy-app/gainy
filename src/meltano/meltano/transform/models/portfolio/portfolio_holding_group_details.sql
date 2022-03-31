{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      'create unique index if not exists "profile_id__ticker_symbol" ON {{ this }} (profile_id, ticker_symbol)',
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}

select concat(profile_holdings_normalized.profile_id, '_',
              portfolio_securities_normalized.ticker_symbol)::varchar as id,
       profile_holdings_normalized.profile_id,
       portfolio_securities_normalized.ticker_symbol,
       min(portfolio_holding_details.purchase_date)                   as purchase_date,
       min(portfolio_holding_group_gains.relative_gain_total)         as relative_gain_total,
       min(portfolio_holding_group_gains.relative_gain_1d)            as relative_gain_1d,
       min(portfolio_holding_group_gains.value_to_portfolio_value)    as value_to_portfolio_value,
       coalesce(min(base_tickers.name),
                min(portfolio_securities_normalized.name))::varchar   as ticker_name,
       min(market_capitalization)                                     as market_capitalization,
       min(next_earnings_date)                                        as next_earnings_date,
       sum(portfolio_holding_details.ltt_quantity_total)              as ltt_quantity_total,
       now()                                                          as updated_at
from {{ ref('portfolio_holding_details') }}
         join {{ ref('profile_holdings_normalized') }}
              on profile_holdings_normalized.holding_id = portfolio_holding_details.holding_id
         join {{ ref('portfolio_securities_normalized') }}
              on portfolio_securities_normalized.id = profile_holdings_normalized.security_id
         join {{ ref('portfolio_holding_group_gains') }}
              on portfolio_holding_group_gains.profile_id = profile_holdings_normalized.profile_id
                  and portfolio_holding_group_gains.ticker_symbol = portfolio_securities_normalized.ticker_symbol
        left join {{ ref('base_tickers') }} 
              on base_tickers.symbol = portfolio_securities_normalized.ticker_symbol
group by profile_holdings_normalized.profile_id, portfolio_securities_normalized.ticker_symbol