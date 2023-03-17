with holdings as
         (
             select distinct on (
                 holding_id_v2
                 )  profile_holdings_normalized_all.profile_id,
                    holding_id_v2,
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
     portfolio_holding_gains as
         (
             select holding_id_v2,
                    actual_value,
                    relative_gain_1d / actual_value as relative_gain_1d,
                    absolute_gain_1d
             from (
                      select holding_id_v2,
                             sum(actual_value)                                            as actual_value,
                             sum(portfolio_holding_gains.relative_gain_1d * actual_value) as relative_gain_1d,
                             sum(absolute_gain_1d)                                        as absolute_gain_1d
                      from portfolio_holding_gains
                      group by holding_id_v2
                  ) t
     ),
     data as materialized
         (
             select holdings.profile_id,
                    holding_id_v2,
                    case
                        when type = 'cash'
                            then 'Cash'
                        end     as name,
                    sim_dif + 1 as weight_interest_in_holding,
                    interest_id,
                    coalesce(case
                        when type = 'cash'
                            then pending_cash
                        end, 0) + actual_value as actual_value,
                    absolute_gain_1d,
                    relative_gain_1d
             from holdings
                      join portfolio_holding_gains using (holding_id_v2)
                      left join trading_profile_status using (profile_id)
                      left join ticker_interests on ticker_interests.symbol = ticker_symbol
             where collection_id is null

             union all

             select holdings.profile_id,
                    holding_id_v2,
                    null        as name,
                    sim_dif + 1 as weight_interest_in_holding,
                    interest_id,
                    actual_value,
                    absolute_gain_1d,
                    relative_gain_1d
             from holdings
                      join portfolio_holding_gains using (holding_id_v2)
                      join collection_interests using (collection_id)
             where collection_id is not null
     ),
     portfolio_stats as
         (
             select profile_id,
                    sum(actual_value) as actual_value_sum
             from (
                      select distinct on (
                          profile_id,
                          holding_id_v2
                          ) profile_id,
                            holding_id_v2,
                            actual_value
                      from data
                  ) t
             group by profile_id
     ),
     portfolio_symbol_stats as
         (
             select profile_id,
                    holding_id_v2,
                    sum(weight_interest_in_holding) as weight_interest_in_holding_sum
             from data
             group by profile_id, holding_id_v2
     ),
     data2 as
         (
             select profile_id,
                    name,
                    actual_value,
                    actual_value / actual_value_sum as weight_holding_in_portfolio,
                    coalesce(
                                weight_interest_in_holding / weight_interest_in_holding_sum,
                                1)                  as weight_interest_in_holding,
                    interest_id,
                    absolute_gain_1d,
                    relative_gain_1d
             from data
                      join portfolio_stats using (profile_id)
                      join portfolio_symbol_stats using (profile_id, holding_id_v2)
             where actual_value_sum > 0
               and (weight_interest_in_holding_sum > 0 or weight_interest_in_holding_sum is null)
     ),
     data3 as
         (
             select profile_id,
                    interest_id,
                    name,
                    sum(weight_holding_in_portfolio * weight_interest_in_holding) as weight,
                    sum(weight_interest_in_holding * absolute_gain_1d)            as absolute_daily_change,
                    sum(weight_holding_in_portfolio * weight_interest_in_holding *
                        relative_gain_1d)                                         as relative_daily_change,
                    sum(weight_interest_in_holding * actual_value)                as absolute_value
             from data2
             group by profile_id, interest_id, name
             having sum(weight_holding_in_portfolio * weight_interest_in_holding) > 0
     )
select profile_id,
       weight::double precision,
       'interest'::varchar                           as entity_type,
       interest_id::varchar                          as entity_id,
       coalesce(data3.name, interests.name, 'Other') as entity_name,
       absolute_daily_change::double precision,
       relative_daily_change::double precision,
       absolute_value::double precision
from data3
         left join interests on interests.id = interest_id
