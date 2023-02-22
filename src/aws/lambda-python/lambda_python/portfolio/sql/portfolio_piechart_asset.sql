with holdings as
         (
             select profile_holdings_normalized_all.profile_id,
                    holding_id_v2,
                    quantity_norm_for_valuation,
                    plaid_access_token_id,
                    type,
                    profile_holdings_normalized_all.ticker_symbol,
                    profile_holdings_normalized_all.collection_id,
                    profile_holdings_normalized_all.symbol
             from profile_holdings_normalized_all
                   {join_clause}
             where profile_id = %(profile_id)s
                   {where_clause}
         ),
     portfolio_assets as materialized
         (
             select holdings.profile_id,
                    'ticker:' || holdings.ticker_symbol                          as entity_id,
                    case
                        when type = 'cash'
                            then 'Cash'
                        else ticker_name
                        end                                                      as entity_name,
                    sum(actual_value)                                            as weight,
                    sum(portfolio_holding_gains.relative_gain_1d * actual_value) as relative_daily_change,
                    sum(absolute_gain_1d)                                        as absolute_daily_change,
                    sum(actual_value)                                            as absolute_value
             from holdings
                      left join portfolio_holding_gains using (holding_id_v2)
                      left join portfolio_holding_details using (holding_id_v2)
             where collection_id is null
             group by holdings.profile_id, holdings.ticker_symbol, ticker_name, type

             union all

             select holdings.profile_id,
                    'collection:' || holdings.collection_id                      as entity_id,
                    collections.name                                             as entity_name,
                    sum(actual_value)                                            as weight,
                    sum(portfolio_holding_gains.relative_gain_1d * actual_value) as relative_daily_change,
                    sum(absolute_gain_1d)                                        as absolute_daily_change,
                    sum(actual_value)                                            as absolute_value
             from holdings
                      join portfolio_holding_gains using (holding_id_v2)
                      join portfolio_holding_details using (holding_id_v2)
                      join collections on collections.id = collection_id
             where collection_id is not null
             group by holdings.profile_id, holdings.collection_id, collections.name
     ),
     portfolio_assets_weight_sum as
         (
             select profile_id,
                    sum(weight) as weight_sum
             from portfolio_assets
             group by profile_id
     )
select portfolio_assets.profile_id,
       weight / weight_sum                             as weight,
       'asset'::varchar                                as entity_type,
       entity_id,
       entity_name,
       coalesce(absolute_daily_change, 0)              as absolute_daily_change,
       coalesce(relative_daily_change / weight_sum, 0) as relative_daily_change,
       absolute_value
from portfolio_assets
         join portfolio_assets_weight_sum using (profile_id)
where weight is not null
  and weight_sum > 0
