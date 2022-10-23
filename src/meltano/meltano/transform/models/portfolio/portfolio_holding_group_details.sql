{{
  config(
    materialized = "incremental",
    unique_key = "holding_group_id",
    tags = ["realtime"],
    post_hook=[
      pk('holding_group_id'),
      'create unique index if not exists "profile_id__ticker_symbol" ON {{ this }} (profile_id, ticker_symbol)',
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}

select profile_holdings_normalized.holding_group_id,
       profile_holdings_normalized.profile_id,
       null as ticker_symbol,
       collection_id,
       min(portfolio_holding_details.purchase_date)                as purchase_date,
       min(portfolio_holding_group_gains.relative_gain_total)      as relative_gain_total,
       min(portfolio_holding_group_gains.relative_gain_1d)         as relative_gain_1d,
       min(portfolio_holding_group_gains.value_to_portfolio_value) as value_to_portfolio_value,
       null                                                        as ticker_name,
       min(collections.name)                                       as name,
       null                                                        as market_capitalization,
       null                                                        as next_earnings_date,
       null                                                        as ltt_quantity_total,
       now()                                                       as updated_at
from {{ ref('profile_holdings_normalized') }}
         join {{ ref('portfolio_holding_details') }} using (holding_id_v2)
         join {{ ref('portfolio_holding_group_gains') }} using (profile_id, collection_id)
         join {{ ref('collections') }} on collections.id = collection_id
group by profile_holdings_normalized.holding_group_id, profile_holdings_normalized.profile_id, collection_id

union all

select profile_holdings_normalized.holding_group_id,
       profile_holdings_normalized.profile_id,
       portfolio_securities_normalized.ticker_symbol,
       null::int as collection_id,
       min(portfolio_holding_details.purchase_date)                as purchase_date,
       min(portfolio_holding_group_gains.relative_gain_total)      as relative_gain_total,
       min(portfolio_holding_group_gains.relative_gain_1d)         as relative_gain_1d,
       min(portfolio_holding_group_gains.value_to_portfolio_value) as value_to_portfolio_value,
       coalesce(min(base_tickers.name),
                min(portfolio_securities_normalized.name))         as ticker_name,
       coalesce(min(base_tickers.name),
                min(portfolio_securities_normalized.name))         as name,
       min(market_capitalization)                                  as market_capitalization,
       min(next_earnings_date)                                     as next_earnings_date,
       sum(portfolio_holding_details.ltt_quantity_total)           as ltt_quantity_total,
       now()                                                       as updated_at
from {{ ref('portfolio_holding_details') }}
         join {{ ref('profile_holdings_normalized') }} using (holding_id_v2)
         join {{ ref('portfolio_securities_normalized') }}
              on portfolio_securities_normalized.id = profile_holdings_normalized.security_id
         join {{ ref('portfolio_holding_group_gains') }}
              on portfolio_holding_group_gains.profile_id = profile_holdings_normalized.profile_id
                  and portfolio_holding_group_gains.ticker_symbol = portfolio_securities_normalized.ticker_symbol
        left join {{ ref('base_tickers') }}
              on base_tickers.symbol = portfolio_securities_normalized.ticker_symbol
group by profile_holdings_normalized.holding_group_id, profile_holdings_normalized.profile_id, portfolio_securities_normalized.ticker_symbol
