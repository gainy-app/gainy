with holdings as
         (
             select profile_holdings_normalized_all.profile_id,
                    holding_id_v2,
                    quantity_norm_for_valuation,
                    plaid_access_token_id,
                    profile_holdings_normalized_all.ticker_symbol,
                    profile_holdings_normalized_all.collection_id,
                    profile_holdings_normalized_all.symbol,
                    case
                        when is_app_trading and collection_id is not null
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

     portfolio_tickers as
         (
             select holdings.profile_id,
                    holdings.ticker_symbol                    as symbol,
                    ticker_name,
                    sum(actual_value)                         as weight,
                    sum(ticker_realtime_metrics.absolute_daily_change *
                        holdings.quantity_norm_for_valuation) as absolute_daily_change,
                    sum(actual_value)                         as absolute_value
             from holdings
                      join portfolio_holding_gains using (holding_id_v2)
                      join portfolio_holding_details using (holding_id_v2)
                      left join ticker_realtime_metrics on ticker_realtime_metrics.symbol = holdings.ticker_symbol
             group by holdings.profile_id, holdings.ticker_symbol, ticker_name
     ),
     portfolio_tickers_weight_sum as
         (
             select profile_id,
                    sum(weight)                as weight_sum,
                    sum(absolute_daily_change) as absolute_daily_change_sum
             from portfolio_tickers
             group by profile_id
     ),
     portfolio_security_types as
         (
             select holdings.profile_id,
                    holdings.security_type,
                    sum(actual_value)                         as weight,
                    sum(ticker_realtime_metrics.absolute_daily_change *
                        holdings.quantity_norm_for_valuation) as absolute_daily_change,
                    sum(actual_value)                         as absolute_value
             from holdings
                      join portfolio_holding_gains using (holding_id_v2)
                      left join ticker_realtime_metrics using (symbol)
             group by holdings.profile_id, holdings.security_type
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
