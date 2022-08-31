with portfolio_tickers as
         (
             select profile_holdings_normalized.profile_id,
                    portfolio_securities_normalized.ticker_symbol                as symbol,
                    sum(actual_value)                                            as weight,
                    sum(ticker_realtime_metrics.absolute_daily_change *
                        profile_holdings_normalized.quantity_norm_for_valuation) as absolute_daily_change,
                    sum(actual_value)                                            as absolute_value
             from profile_holdings_normalized
                      join portfolio_securities_normalized
                           on portfolio_securities_normalized.id = security_id
                      join portfolio_holding_gains using (holding_id)
                      left join ticker_realtime_metrics
                                on ticker_realtime_metrics.symbol =
                                   portfolio_securities_normalized.original_ticker_symbol
                      join app.profile_plaid_access_tokens
                           on profile_plaid_access_tokens.id = profile_holdings_normalized.plaid_access_token_id
             where {where_clause}
             group by profile_holdings_normalized.profile_id, portfolio_securities_normalized.ticker_symbol
         ),
     portfolio_tickers_weight_sum as
         (
             select profile_id,
                    sum(weight)                as weight_sum,
                    sum(absolute_daily_change) as absolute_daily_change_sum
             from portfolio_tickers
             group by profile_id
         )
select portfolio_tickers.profile_id,
       weight / weight_sum                as weight,
       'ticker'::varchar                  as entity_type,
       symbol                             as entity_id,
       ticker_name                        as entity_name,
       coalesce(absolute_daily_change, 0) as absolute_daily_change,
       case
           when abs(absolute_value - absolute_daily_change) > 0
               then absolute_value / (absolute_value - absolute_daily_change) - 1
           end                            as relative_daily_change,
       absolute_value
from portfolio_tickers
         join portfolio_tickers_weight_sum using (profile_id)
         join portfolio_holding_group_details
              on portfolio_holding_group_details.ticker_symbol = portfolio_tickers.symbol
                  and portfolio_holding_group_details.profile_id = portfolio_tickers.profile_id
where weight is not null
  and weight_sum > 0

union all

(
    with data as materialized
             (
                 select profile_holdings_normalized.profile_id,
                        original_ticker_symbol as symbol,
                        sim_dif + 1 as weight_category_in_symbol,
                        category_id,
                        profile_holdings_normalized.quantity_norm_for_valuation,
                        actual_value
                 from profile_holdings_normalized
                          join portfolio_securities_normalized
                               on portfolio_securities_normalized.id = security_id
                          join portfolio_holding_gains using (holding_id)
                          left join ticker_realtime_metrics
                                    on ticker_realtime_metrics.symbol =
                                       portfolio_securities_normalized.original_ticker_symbol
                          join ticker_categories_continuous
                               on ticker_categories_continuous.symbol = portfolio_securities_normalized.ticker_symbol
                          join app.profile_plaid_access_tokens
                               on profile_plaid_access_tokens.id = profile_holdings_normalized.plaid_access_token_id
                 where {where_clause}
             ),
         portfolio_stats as
             (
                 select profile_id,
                        sum(actual_value) as actual_value_sum
                 from (
                          select distinct on (
                              profile_id,
                              symbol
                              ) profile_id,
                                symbol,
                                actual_value
                          from data
                      ) t
                 group by profile_id
             ),
         portfolio_symbol_stats as
             (
                 select profile_id,
                        symbol,
                        sum(weight_category_in_symbol) as weight_category_in_symbol_sum
                 from data
                 group by profile_id, symbol
             ),
         data2 as
             (
                 select profile_id,
                        symbol,
                        actual_value / actual_value_sum                           as weight_symbol_in_portfolio,
                        weight_category_in_symbol / weight_category_in_symbol_sum as weight_category_in_symbol,
                        category_id,
                        categories.name,
                        quantity_norm_for_valuation,
                        absolute_daily_change,
                        actual_price,
                        previous_day_close_price
                 from data
                          join portfolio_stats using (profile_id)
                          join portfolio_symbol_stats using (profile_id, symbol)
                          join categories on categories.id = category_id
                          join ticker_realtime_metrics using (symbol)
                 where actual_value_sum > 0
                   and weight_category_in_symbol_sum > 0
             )
    select profile_id,
           sum(weight_symbol_in_portfolio * weight_category_in_symbol)::double precision                as weight,
           'category'::varchar                                                                          as entity_type,
           category_id::varchar                                                                         as entity_id,
           min(name)                                                                                    as entity_name,
           sum(quantity_norm_for_valuation * weight_category_in_symbol *
               absolute_daily_change)::double precision                                                 as absolute_daily_change,
           (sum(quantity_norm_for_valuation * weight_category_in_symbol * actual_price) /
            sum(quantity_norm_for_valuation * weight_category_in_symbol * coalesce(previous_day_close_price, actual_price)) -
            1)::double precision                                                                        as relative_daily_change,
           sum(quantity_norm_for_valuation * weight_category_in_symbol * actual_price)::double precision as absolute_value
    from data2
    group by profile_id, category_id
)

union all

(
    with data as materialized
             (
                 select profile_holdings_normalized.profile_id,
                        original_ticker_symbol as symbol,
                        sim_dif + 1 as weight_interest_in_symbol,
                        interest_id,
                        profile_holdings_normalized.quantity_norm_for_valuation,
                        actual_value
                 from profile_holdings_normalized
                          join portfolio_securities_normalized
                               on portfolio_securities_normalized.id = security_id
                          join portfolio_holding_gains using (holding_id)
                          left join ticker_realtime_metrics
                                    on ticker_realtime_metrics.symbol =
                                       portfolio_securities_normalized.original_ticker_symbol
                          join ticker_interests
                               on ticker_interests.symbol = portfolio_securities_normalized.ticker_symbol
                          join app.profile_plaid_access_tokens
                               on profile_plaid_access_tokens.id = profile_holdings_normalized.plaid_access_token_id
                 where {where_clause}
             ),
         portfolio_stats as
             (
                 select profile_id,
                        sum(actual_value) as actual_value_sum
                 from (
                          select distinct on (
                              profile_id,
                              symbol
                              ) profile_id,
                                symbol,
                                actual_value
                          from data
                      ) t
                 group by profile_id
             ),
         portfolio_symbol_stats as
             (
                 select profile_id,
                        symbol,
                        sum(weight_interest_in_symbol) as weight_interest_in_symbol_sum
                 from data
                 group by profile_id, symbol
             ),
         data2 as
             (
                 select profile_id,
                        symbol,
                        actual_value / actual_value_sum                           as weight_symbol_in_portfolio,
                        weight_interest_in_symbol / weight_interest_in_symbol_sum as weight_interest_in_symbol,
                        interest_id,
                        interests.name,
                        quantity_norm_for_valuation,
                        absolute_daily_change,
                        actual_price,
                        previous_day_close_price
                 from data
                          join portfolio_stats using (profile_id)
                          join portfolio_symbol_stats using (profile_id, symbol)
                          join interests on interests.id = interest_id
                          join ticker_realtime_metrics using (symbol)
                 where actual_value_sum > 0
                   and weight_interest_in_symbol_sum > 0
             )
    select profile_id,
           sum(weight_symbol_in_portfolio * weight_interest_in_symbol)::double precision                 as weight,
           'interest'::varchar                                                                           as entity_type,
           interest_id::varchar                                                                          as entity_id,
           min(name)                                                                                     as entity_name,
           sum(quantity_norm_for_valuation * weight_interest_in_symbol *
               absolute_daily_change)::double precision                                                  as absolute_daily_change,
           (sum(quantity_norm_for_valuation * weight_interest_in_symbol * actual_price) /
            sum(quantity_norm_for_valuation * weight_interest_in_symbol * coalesce(previous_day_close_price, actual_price)) -
            1)::double precision                                                                         as relative_daily_change,
           sum(quantity_norm_for_valuation * weight_interest_in_symbol * actual_price)::double precision as absolute_value
    from data2
    group by profile_id, interest_id
)

union all

(
    with portfolio_security_types as
             (
                 select profile_holdings_normalized.profile_id,
                        portfolio_securities_normalized.type                         as security_type,
                        sum(actual_value)                                            as weight,
                        sum(ticker_realtime_metrics.absolute_daily_change *
                            profile_holdings_normalized.quantity_norm_for_valuation) as absolute_daily_change,
                        sum(actual_value)                                            as absolute_value
                 from profile_holdings_normalized
                          join portfolio_securities_normalized
                               on portfolio_securities_normalized.id = security_id
                          join portfolio_holding_gains using (holding_id)
                          left join ticker_realtime_metrics
                                    on ticker_realtime_metrics.symbol =
                                       portfolio_securities_normalized.original_ticker_symbol
                          join app.profile_plaid_access_tokens
                               on profile_plaid_access_tokens.id = profile_holdings_normalized.plaid_access_token_id
                 where {where_clause}
                 group by profile_holdings_normalized.profile_id, portfolio_securities_normalized.type
             ),
         portfolio_security_types_weight_sum as
             (
                 select profile_id,
                        sum(weight) as weight_sum
                 from portfolio_security_types
                 group by profile_id
             )
    select profile_id,
           weight,
           entity_type,
           entity_id,
           entity_name,
           absolute_daily_change,
           case
               when abs(absolute_value - absolute_daily_change) > 0
                   then absolute_value / (absolute_value - absolute_daily_change) - 1
               end as relative_daily_change,
           absolute_value
    from (
             select portfolio_security_types.profile_id,
                    weight / portfolio_security_types_weight_sum.weight_sum as weight,
                    'security_type'::varchar                                as entity_type,
                    security_type                                           as entity_id,
                    security_type                                           as entity_name,
                    coalesce(absolute_daily_change, 0)                      as absolute_daily_change,
                    absolute_value * portfolio_tickers_weight_sum.weight_sum /
                    portfolio_security_types_weight_sum.weight_sum          as absolute_value
             from portfolio_security_types
                      join portfolio_tickers_weight_sum using (profile_id)
                      join portfolio_security_types_weight_sum using (profile_id)
             where weight is not null
               and portfolio_security_types_weight_sum.weight_sum > 0
         ) t
)

union all

(
    with data as materialized
             (
                 select profile_holdings_normalized.profile_id,
                        original_ticker_symbol as symbol,
                        weight                 as weight_collection_in_symbol,
                        collection_id,
                        profile_holdings_normalized.quantity_norm_for_valuation,
                        actual_value
                 from profile_holdings_normalized
                          join portfolio_securities_normalized
                               on portfolio_securities_normalized.id = security_id
                          join portfolio_holding_gains using (holding_id)
                          left join ticker_realtime_metrics
                                    on ticker_realtime_metrics.symbol =
                                       portfolio_securities_normalized.original_ticker_symbol
                          join ticker_collections
                               on ticker_collections.symbol = portfolio_securities_normalized.ticker_symbol
                          join app.profile_plaid_access_tokens
                               on profile_plaid_access_tokens.id = profile_holdings_normalized.plaid_access_token_id
                 where {where_clause}
             ),
         portfolio_stats as
             (
                 select profile_id,
                        sum(actual_value) as actual_value_sum
                 from (
                          select distinct on (
                              profile_id,
                              symbol
                              ) profile_id,
                                symbol,
                                actual_value
                          from data
                      ) t
                 group by profile_id
             ),
         portfolio_symbol_stats as
             (
                 select profile_id,
                        symbol,
                        sum(weight_collection_in_symbol) as weight_collection_in_symbol_sum
                 from data
                 group by profile_id, symbol
             ),
         data2 as
             (
                 select profile_id,
                        symbol,
                        actual_value / actual_value_sum                           as weight_symbol_in_portfolio,
                        weight_collection_in_symbol / weight_collection_in_symbol_sum as weight_collection_in_symbol,
                        collection_id,
                        collections.name,
                        quantity_norm_for_valuation,
                        absolute_daily_change,
                        actual_price,
                        previous_day_close_price
                 from data
                          join portfolio_stats using (profile_id)
                          join portfolio_symbol_stats using (profile_id, symbol)
                          join collections on collections.id = collection_id
                          join ticker_realtime_metrics using (symbol)
                 where actual_value_sum > 0
                   and weight_collection_in_symbol_sum > 0
             )
    select profile_id,
           sum(weight_symbol_in_portfolio * weight_collection_in_symbol)::double precision                as weight,
           'collection'::varchar                                                                          as entity_type,
           collection_id::varchar                                                                         as entity_id,
           min(name)                                                                                    as entity_name,
           sum(quantity_norm_for_valuation * weight_collection_in_symbol *
               absolute_daily_change)::double precision                                                 as absolute_daily_change,
           (sum(quantity_norm_for_valuation * weight_collection_in_symbol * actual_price) /
            sum(quantity_norm_for_valuation * weight_collection_in_symbol * coalesce(previous_day_close_price, actual_price)) -
            1)::double precision                                                                        as relative_daily_change,
           sum(quantity_norm_for_valuation * weight_collection_in_symbol * actual_price)::double precision as absolute_value
    from data2
    group by profile_id, collection_id
)
