with holdings as
         (
             select distinct on (
                 holding_id_v2
                 )  profile_holdings_normalized_all.profile_id,
                    holding_id_v2,
                    holding_group_id,
                    plaid_access_token_id,
                    profile_holdings_normalized_all.ticker_symbol,
                    profile_holdings_normalized_all.collection_id,
                    profile_holdings_normalized_all.symbol,
                    case
                        when is_app_trading and profile_holdings_normalized_all.collection_id is not null
                            then 'TTF'
                        when profile_holdings_normalized_all.type = 'etf'
                            then upper(profile_holdings_normalized_all.type)
                            else initcap(profile_holdings_normalized_all.type)
                        end as security_type
             from profile_holdings_normalized_all
                   {join_clause}
             where profile_id = %(profile_id)s
                   {where_clause}
         ),
     portfolio_security_types as
         (
             select holdings.profile_id,
                    holdings.security_type,
                    sum(coalesce(case
                        when security_type = 'Cash'
                            then pending_cash
                        end, 0) +
                        portfolio_holding_gains.actual_value)       as weight,
                    sum(portfolio_holding_group_gains.relative_gain_1d *
                        portfolio_holding_gains.actual_value /
                        portfolio_holding_group_gains.actual_value) as relative_daily_change,
                    sum(portfolio_holding_group_gains.absolute_gain_1d *
                        portfolio_holding_gains.actual_value /
                        portfolio_holding_group_gains.actual_value) as absolute_daily_change,
                    sum(coalesce(case
                                     when security_type = 'Cash'
                                         then pending_cash
                                     end, 0) +
                        portfolio_holding_gains.actual_value)       as absolute_value
             from holdings
                      join portfolio_holding_gains using (profile_id, holding_id_v2)
                      left join portfolio_holding_group_gains using (profile_id, holding_group_id)
                      left join trading_profile_status using (profile_id)
             group by holdings.profile_id, holdings.security_type
         ),
     portfolio_security_types_weight_sum as
         (
             select profile_id,
                    sum(weight) as weight_sum
             from portfolio_security_types
             group by profile_id
     )
select portfolio_security_types.profile_id,
       weight / weight_sum                as weight,
       'security_type'                    as entity_type,
       security_type                      as entity_id,
       security_type                      as entity_name,
       coalesce(absolute_daily_change, 0) as absolute_daily_change,
       coalesce(relative_daily_change, 0) as relative_daily_change,
       absolute_value
from portfolio_security_types
         join portfolio_security_types_weight_sum using (profile_id)
where weight is not null
  and portfolio_security_types_weight_sum.weight_sum > 0
