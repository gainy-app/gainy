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
    with portfolio_categories as
             (
                 select profile_holdings_normalized.profile_id,
                        category_id,
                        sum(absolute_daily_change *
                            profile_holdings_normalized.quantity_norm_for_valuation) as absolute_daily_change,
                        sum(actual_value)                                            as absolute_value
                 from profile_holdings_normalized
                          join portfolio_securities_normalized
                               on portfolio_securities_normalized.id = security_id
                          join portfolio_holding_gains using (holding_id)
                          left join ticker_realtime_metrics
                                    on ticker_realtime_metrics.symbol =
                                       portfolio_securities_normalized.original_ticker_symbol
                          join ticker_categories
                               on ticker_categories.symbol = portfolio_securities_normalized.ticker_symbol
                          join app.profile_plaid_access_tokens
                               on profile_plaid_access_tokens.id = profile_holdings_normalized.plaid_access_token_id
                 where {where_clause}
                 group by profile_holdings_normalized.profile_id, category_id
             ),
         portfolio_categories_weight_sum_actual as
             (
                 select profile_id,
                        sum(absolute_value)        as weight_sum,
                        sum(absolute_daily_change) as absolute_daily_change_sum
                 from portfolio_categories
                 group by profile_id
             ),
         portfolio_categories_weight_sum_target as
             (
                 select profile_id,
                        sum(weight)                as weight_sum,
                        sum(absolute_daily_change) as absolute_daily_change_sum
                 from (
                          select distinct on (
                              profile_id, symbol
                              ) profile_id,
                                weight,
                                absolute_daily_change
                          from portfolio_tickers
                                   join ticker_categories using (symbol)
                      ) t
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
             select portfolio_categories.profile_id,
                    absolute_value / portfolio_categories_weight_sum_actual.weight_sum as weight,
                    'category'::varchar                                                as entity_type,
                    category_id::varchar                                               as entity_id,
                    categories.name                                                    as entity_name,
                    case
                        when abs(portfolio_categories_weight_sum_actual.absolute_daily_change_sum) > 0
                            then portfolio_categories_weight_sum_target.absolute_daily_change_sum /
                                 portfolio_categories_weight_sum_actual.absolute_daily_change_sum
                        else 1
                        end * absolute_daily_change                                    as absolute_daily_change,
                    absolute_value * portfolio_categories_weight_sum_target.weight_sum /
                    portfolio_categories_weight_sum_actual.weight_sum                  as absolute_value
             from portfolio_categories
                      join portfolio_categories_weight_sum_actual using (profile_id)
                      join portfolio_categories_weight_sum_target using (profile_id)
                      join categories on portfolio_categories.category_id = categories.id
             where portfolio_categories_weight_sum_actual.weight_sum > 0
         ) t
)

union all

(
    with portfolio_interests as
             (
                 select profile_holdings_normalized.profile_id,
                        interest_id,
                        sum(absolute_daily_change *
                            profile_holdings_normalized.quantity_norm_for_valuation) as absolute_daily_change,
                        sum(actual_value)                                            as absolute_value
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
                 group by profile_holdings_normalized.profile_id, interest_id
             ),
         portfolio_interests_weight_sum_actual as
             (
                 select profile_id,
                        sum(absolute_value)        as weight_sum,
                        sum(absolute_daily_change) as absolute_daily_change_sum
                 from portfolio_interests
                 group by profile_id
             ),
         portfolio_interests_weight_sum_target as
             (
                 select profile_id,
                        sum(weight)                as weight_sum,
                        sum(absolute_daily_change) as absolute_daily_change_sum
                 from (
                          select distinct on (
                              profile_id, symbol
                              ) profile_id,
                                weight,
                                absolute_daily_change
                          from portfolio_tickers
                                   join ticker_interests using (symbol)
                      ) t
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
             select portfolio_interests.profile_id,
                    absolute_value / portfolio_interests_weight_sum_actual.weight_sum as weight,
                    'interest'::varchar                                                as entity_type,
                    interest_id::varchar                                               as entity_id,
                    interests.name                                                    as entity_name,
                    case
                        when abs(portfolio_interests_weight_sum_actual.absolute_daily_change_sum) > 0
                            then portfolio_interests_weight_sum_target.absolute_daily_change_sum /
                                 portfolio_interests_weight_sum_actual.absolute_daily_change_sum
                        else 1
                        end * absolute_daily_change                                    as absolute_daily_change,
                    absolute_value * portfolio_interests_weight_sum_target.weight_sum /
                    portfolio_interests_weight_sum_actual.weight_sum                  as absolute_value
             from portfolio_interests
                      join portfolio_interests_weight_sum_actual using (profile_id)
                      join portfolio_interests_weight_sum_target using (profile_id)
                      join interests on portfolio_interests.interest_id = interests.id
             where portfolio_interests_weight_sum_actual.weight_sum > 0
         ) t
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
    with portfolio_collections as
             (
                 select profile_holdings_normalized.profile_id,
                        collection_id,
                        sum(absolute_daily_change *
                            profile_holdings_normalized.quantity_norm_for_valuation) as absolute_daily_change,
                        sum(actual_value)                                            as absolute_value
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
                 group by profile_holdings_normalized.profile_id, collection_id
             ),
         portfolio_collections_weight_sum_actual as
             (
                 select profile_id,
                        sum(absolute_value)        as weight_sum,
                        sum(absolute_daily_change) as absolute_daily_change_sum
                 from portfolio_collections
                 group by profile_id
             ),
         portfolio_collections_weight_sum_target as
             (
                 select profile_id,
                        sum(weight)                as weight_sum,
                        sum(absolute_daily_change) as absolute_daily_change_sum
                 from (
                          select distinct on (
                              profile_id, symbol
                              ) profile_id,
                                weight,
                                absolute_daily_change
                          from portfolio_tickers
                                   join ticker_collections using (symbol)
                      ) t
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
             select portfolio_collections.profile_id,
                    absolute_value / portfolio_collections_weight_sum_actual.weight_sum as weight,
                    'collection'::varchar                                                as entity_type,
                    collection_id::varchar                                               as entity_id,
                    collections.name                                                    as entity_name,
                    case
                        when abs(portfolio_collections_weight_sum_actual.absolute_daily_change_sum) > 0
                            then portfolio_collections_weight_sum_target.absolute_daily_change_sum /
                                 portfolio_collections_weight_sum_actual.absolute_daily_change_sum
                        else 1
                        end * absolute_daily_change                                    as absolute_daily_change,
                    absolute_value * portfolio_collections_weight_sum_target.weight_sum /
                    portfolio_collections_weight_sum_actual.weight_sum                  as absolute_value
             from portfolio_collections
                      join portfolio_collections_weight_sum_actual using (profile_id)
                      join portfolio_collections_weight_sum_target using (profile_id)
                      join collections on portfolio_collections.collection_id = collections.id
             where portfolio_collections_weight_sum_actual.weight_sum > 0
         ) t
)
