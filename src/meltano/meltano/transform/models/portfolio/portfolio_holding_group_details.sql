{{
  config(
    materialized = "view",
  )
}}

select t.holding_group_id,
       t.profile_id,
       null                                                   as ticker_symbol,            -- deprecated
       t.collection_id,                                                                    -- deprecated
       t.purchase_date,
       portfolio_holding_group_gains.relative_gain_total      as relative_gain_total,      -- deprecated
       portfolio_holding_group_gains.relative_gain_1d         as relative_gain_1d,         -- deprecated
       portfolio_holding_group_gains.value_to_portfolio_value as value_to_portfolio_value, -- deprecated
       null                                                   as ticker_name,              -- deprecated
       collections.name                                       as name,
       null                                                   as market_capitalization,
       null                                                   as next_earnings_date,
       null                                                   as ltt_quantity_total,
       now()                                                  as updated_at
from (
         select profile_holdings_normalized_dynamic.holding_group_id,
                profile_holdings_normalized_dynamic.profile_id,
                profile_holdings_normalized_dynamic.collection_id,
                min(profile_holdings_normalized_all.holding_since) as purchase_date
         from {{ ref('profile_holdings_normalized_dynamic') }}
                  left join {{ ref('profile_holdings_normalized_all') }} using (holding_id_v2)
         where profile_holdings_normalized_dynamic.collection_id is not null
           and not profile_holdings_normalized_dynamic.is_hidden
         group by profile_holdings_normalized_dynamic.holding_group_id,
             profile_holdings_normalized_dynamic.profile_id,
             profile_holdings_normalized_dynamic.collection_id
     ) t
         left join {{ ref('portfolio_holding_group_gains') }} using (profile_id, collection_id)
         join {{ ref('collections') }} on collections.id = t.collection_id

union all

select profile_holdings_normalized_dynamic.holding_group_id,
       profile_holdings_normalized_dynamic.profile_id,
       profile_holdings_normalized_dynamic.ticker_symbol,
       null::int as collection_id,
       min(profile_holdings_normalized_all.holding_since)          as purchase_date,
       min(portfolio_holding_group_gains.relative_gain_total)      as relative_gain_total,
       min(portfolio_holding_group_gains.relative_gain_1d)         as relative_gain_1d,
       min(portfolio_holding_group_gains.value_to_portfolio_value) as value_to_portfolio_value,
       min(profile_holdings_normalized_dynamic.name)               as ticker_name,
       min(profile_holdings_normalized_dynamic.name)               as name,
       min(market_capitalization)                                  as market_capitalization,
       min(next_earnings_date)::timestamp                          as next_earnings_date,
       min(portfolio_holding_group_gains.ltt_quantity_total)       as ltt_quantity_total,
       now()                                                       as updated_at
from {{ ref('profile_holdings_normalized_dynamic') }}
         left join {{ ref('profile_holdings_normalized_all') }} using (holding_id_v2)
         left join {{ ref('portfolio_holding_group_gains') }}
              on portfolio_holding_group_gains.profile_id = profile_holdings_normalized_dynamic.profile_id
                  and portfolio_holding_group_gains.ticker_symbol = profile_holdings_normalized_dynamic.ticker_symbol
         left join {{ ref('base_tickers') }}
              on base_tickers.symbol = profile_holdings_normalized_dynamic.ticker_symbol
         left join {{ ref('ticker_metrics') }}
              on ticker_metrics.symbol = profile_holdings_normalized_dynamic.ticker_symbol
where profile_holdings_normalized_dynamic.collection_id is null
  and not profile_holdings_normalized_dynamic.is_hidden
group by profile_holdings_normalized_dynamic.holding_group_id, profile_holdings_normalized_dynamic.profile_id, profile_holdings_normalized_dynamic.ticker_symbol
