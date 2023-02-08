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
     data as materialized
         (
             select holdings.profile_id,
                    holding_id_v2,
                    sim_dif + 1 as weight_category_in_holding,
                    category_id,
                    holdings.quantity_norm_for_valuation,
                    actual_value,
                    absolute_daily_change,
                    actual_price,
                    previous_day_close_price
             from holdings
                      join portfolio_holding_gains using (holding_id_v2)
                      left join ticker_categories_continuous on ticker_categories_continuous.symbol = ticker_symbol
                      join ticker_realtime_metrics on ticker_realtime_metrics.symbol = holdings.symbol
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
                    sum(weight_category_in_holding) as weight_category_in_holding_sum
             from data
             group by profile_id, holding_id_v2
     ),
     data2 as
         (
             select profile_id,
                    holding_id_v2,
                    actual_value / actual_value_sum as weight_holding_in_portfolio,
                    coalesce(
                                weight_category_in_holding / weight_category_in_holding_sum,
                                1)                  as weight_category_in_holding,
                    category_id,
                    quantity_norm_for_valuation,
                    absolute_daily_change,
                    actual_price,
                    previous_day_close_price
             from data
                      join portfolio_stats using (profile_id)
                      join portfolio_symbol_stats using (profile_id, holding_id_v2)
             where actual_value_sum > 0
               and (weight_category_in_holding_sum > 0 or weight_category_in_holding_sum is null)
     ),
     data3 as
         (
             select profile_id,
                    sum(weight_holding_in_portfolio * weight_category_in_holding)                 as weight,
                    category_id,
                    sum(quantity_norm_for_valuation * weight_category_in_holding *
                        absolute_daily_change)                                                  as absolute_daily_change,
                    sum(quantity_norm_for_valuation * weight_category_in_holding * actual_price) as actual_price,
                    sum(quantity_norm_for_valuation * weight_category_in_holding *
                        coalesce(previous_day_close_price, actual_price))                       as previous_day_close_price,
                    sum(quantity_norm_for_valuation * weight_category_in_holding * actual_price) as absolute_value
             from data2
             group by profile_id, category_id
             having sum(weight_holding_in_portfolio * weight_category_in_holding) > 0
     )
select profile_id,
       weight::double precision,
       'category'::varchar        as entity_type,
       category_id::varchar       as entity_id,
       coalesce(name, 'Other')    as entity_name,
       absolute_daily_change::double precision,
       (case
            when previous_day_close_price > 0
                then actual_price / previous_day_close_price - 1
            else 0
           end)::double precision as relative_daily_change,
       absolute_value::double precision
from data3
         left join categories on categories.id = category_id
