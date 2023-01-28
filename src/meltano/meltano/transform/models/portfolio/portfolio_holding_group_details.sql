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
                min(profile_holdings_normalized_all.holding_sice) as purchase_date
         from {{ ref('profile_holdings_normalized_dynamic') }}
                  left join {{ ref('profile_holdings_normalized_all') }} using (holding_id_v2)
         where collection_id is not null
           and not profile_holdings_normalized_dynamic.is_hidden
         group by holding_group_id, profile_id, collection_id
     ) t
         join {{ ref('portfolio_holding_group_gains') }} using (profile_id, collection_id)
         join {{ ref('collections') }} on collections.id = t.collection_id

union all

select profile_holdings_normalized_dynamic.holding_group_id,
       profile_holdings_normalized_dynamic.profile_id,
       portfolio_securities_normalized.ticker_symbol,
       null::int as collection_id,
       min(profile_holdings_normalized_all.holding_since)                as purchase_date,
       min(portfolio_holding_group_gains.relative_gain_total)      as relative_gain_total,
       min(portfolio_holding_group_gains.relative_gain_1d)         as relative_gain_1d,
       min(portfolio_holding_group_gains.value_to_portfolio_value) as value_to_portfolio_value,
       coalesce(min(base_tickers.name),
                min(portfolio_securities_normalized.name))         as ticker_name,
       coalesce(min(base_tickers.name),
                min(portfolio_securities_normalized.name))         as name,
       min(market_capitalization)                                  as market_capitalization,
       min(next_earnings_date)                                     as next_earnings_date,
       sum(portfolio_holding_gains.ltt_quantity_total)             as ltt_quantity_total,
       now()                                                       as updated_at
from {{ ref('profile_holdings_normalized_dynamic') }}
         left join {{ ref('profile_holdings_normalized_all') }} using (holding_id_v2)
         left join {{ ref('portfolio_holding_gains') }} using (holding_id_v2)
         join {{ ref('portfolio_securities_normalized') }}
              on portfolio_securities_normalized.id = profile_holdings_normalized_dynamic.security_id
         join {{ ref('portfolio_holding_group_gains') }}
              on portfolio_holding_group_gains.profile_id = profile_holdings_normalized_dynamic.profile_id
                  and portfolio_holding_group_gains.ticker_symbol = portfolio_securities_normalized.ticker_symbol
         left join {{ ref('base_tickers') }}
              on base_tickers.symbol = portfolio_securities_normalized.ticker_symbol
where not profile_holdings_normalized_dynamic.is_hidden
group by profile_holdings_normalized_dynamic.holding_group_id, profile_holdings_normalized_dynamic.profile_id, portfolio_securities_normalized.ticker_symbol
