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
     portfolio_tickers as
         (
             select holdings.profile_id,
                    holdings.ticker_symbol as symbol,
                    case
                        when type = 'cash'
                            then 'Cash'
                        else ticker_name
                        end                as ticker_name,
                    sum(actual_value)      as weight,
                    sum(portfolio_holding_gains.relative_gain_1d *
                        actual_value)      as relative_daily_change,
                    sum(absolute_gain_1d)  as absolute_daily_change,
                    sum(actual_value)      as absolute_value
             from holdings
                      join portfolio_holding_gains using (holding_id_v2)
                      left join portfolio_holding_details using (holding_id_v2)
                      left join ticker_realtime_metrics on ticker_realtime_metrics.symbol = holdings.ticker_symbol
             group by holdings.profile_id, holdings.ticker_symbol, ticker_name, type
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
       coalesce(absolute_daily_change, 0)              as absolute_daily_change,
       coalesce(relative_daily_change / weight_sum, 0) as relative_daily_change,
       absolute_value
from portfolio_tickers
         join portfolio_tickers_weight_sum using (profile_id)
where weight is not null
  and weight_sum > 0