{{
  config(
    materialized = "view",
  )
}}

with relative_gains as
         (
             with raw_data_0d as
                      (
                          select distinct on (
                              profile_id, holding_id_v2
                              ) profile_id,
                                holding_id_v2,
                                date,
                                exp(sum(ln(coalesce(relative_gain, 0) + 1 + 1e-10))) - 1 as relative_gain_1d
                          from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
                                   join {{ ref('portfolio_chart_skeleton') }} using (profile_id, datetime)
                          where drivewealth_portfolio_historical_prices_aggregated.period = '3min'
                            and portfolio_chart_skeleton.period = '1d'
                          group by profile_id, holding_id_v2, symbol, date
                          order by profile_id, holding_id_v2, symbol, date desc
                      ),
                  raw_data_1w as
                      (
                          select profile_id,
                                 holding_id_v2,
                                 exp(sum(ln(coalesce(relative_gain, 0) + 1 + 1e-10))) - 1 as relative_gain_1w
                          from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
                                   join {{ ref('portfolio_chart_skeleton') }} using (profile_id, datetime)
                                   left join (
                                                 select profile_id, holding_id_v2, max(date) as last_selloff_date
                                                 from {{ ref('drivewealth_portfolio_historical_holdings') }}
                                                 where date >= now()::date - interval '1 week'
                                                   and value < 1e-3
                                                 group by profile_id, holding_id_v2
                                             ) last_selloff_date using (profile_id, holding_id_v2)
                          where drivewealth_portfolio_historical_prices_aggregated.period = '15min'
                            and portfolio_chart_skeleton.period = '1w'
                            and date >= coalesce(last_selloff_date, now()::date - interval '1 week')
                          group by profile_id, holding_id_v2
                  ),
                  raw_data_1m as
                      (
                          select profile_id,
                                 holding_id_v2,
                                 exp(sum(ln(coalesce(relative_gain, 0) + 1 + 1e-10))) - 1 as relative_gain_1m
                          from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
                                   left join (
                                                 select profile_id, holding_id_v2, max(date) as last_selloff_date
                                                 from {{ ref('drivewealth_portfolio_historical_holdings') }}
                                                 where date >= now()::date - interval '1 month'
                                                   and value < 1e-3
                                                 group by profile_id, holding_id_v2
                                             ) last_selloff_date using (profile_id, holding_id_v2)
                          where period = '1d'
                            and date >= coalesce(last_selloff_date, now()::date - interval '1 month')
                          group by profile_id, holding_id_v2
                  ),
                  raw_data_3m as
                      (
                          select profile_id,
                                 holding_id_v2,
                                 exp(sum(ln(coalesce(relative_gain, 0) + 1 + 1e-10))) - 1 as relative_gain_3m
                          from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
                                   left join (
                                                 select profile_id, holding_id_v2, max(date) as last_selloff_date
                                                 from {{ ref('drivewealth_portfolio_historical_holdings') }}
                                                 where date >= now()::date - interval '3 month'
                                                   and value < 1e-3
                                                 group by profile_id, holding_id_v2
                                             ) last_selloff_date using (profile_id, holding_id_v2)
                          where period = '1d'
                            and date >= coalesce(last_selloff_date, now()::date - interval '3 month')
                          group by profile_id, holding_id_v2
                  ),
                  raw_data_1y as
                      (
                          select profile_id,
                                 holding_id_v2,
                                 exp(sum(ln(coalesce(relative_gain, 0) + 1 + 1e-10))) - 1 as relative_gain_1y
                          from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
                                   left join (
                                                 select profile_id, holding_id_v2, max(date) as last_selloff_date
                                                 from {{ ref('drivewealth_portfolio_historical_holdings') }}
                                                 where date >= now()::date - interval '1 year'
                                                   and value < 1e-3
                                                 group by profile_id, holding_id_v2
                                             ) last_selloff_date using (profile_id, holding_id_v2)
                          where period = '1d'
                            and date >= coalesce(last_selloff_date, now()::date - interval '1 year')
                          group by profile_id, holding_id_v2
                  ),
                  raw_data_5y as
                      (
                          select profile_id,
                                 holding_id_v2,
                                 exp(sum(ln(coalesce(relative_gain, 0) + 1 + 1e-10))) - 1 as relative_gain_5y
                          from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
                                   left join (
                                                 select profile_id, holding_id_v2, max(date) as last_selloff_date
                                                 from {{ ref('drivewealth_portfolio_historical_holdings') }}
                                                 where date >= now()::date - interval '5 year'
                                                   and value < 1e-3
                                                 group by profile_id, holding_id_v2
                                             ) last_selloff_date using (profile_id, holding_id_v2)
                          where period = '1w'
                            and date >= coalesce(last_selloff_date, now()::date - interval '5 year')
                          group by profile_id, holding_id_v2
                  ),
                  raw_data_all as
                      (
                          select profile_id,
                                 holding_id_v2,
                                 exp(sum(ln(coalesce(relative_gain, 0) + 1 + 1e-10))) - 1 as relative_gain_total
                          from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
                                   left join (
                                                 select profile_id, holding_id_v2, max(date) as last_selloff_date
                                                 from {{ ref('drivewealth_portfolio_historical_holdings') }}
                                                 where date >= now()::date - interval '1 month'
                                                   and value < 1e-3
                                                 group by profile_id, holding_id_v2
                                             ) last_selloff_date using (profile_id, holding_id_v2)
                          where period = '1m'
                            and (date >= last_selloff_date or last_selloff_date is null)
                          group by profile_id, holding_id_v2
                  )
             select profile_id,
                    symbol,
                    collection_id,
                    relative_gain_1d,
                    relative_gain_1w,
                    relative_gain_1m,
                    relative_gain_3m,
                    relative_gain_1y,
                    relative_gain_5y,
                    relative_gain_total
             from {{ ref('profile_holdings_normalized_all') }}
                      left join raw_data_0d using (profile_id, holding_id_v2)
                      left join raw_data_1w using (profile_id, holding_id_v2)
                      left join raw_data_1m using (profile_id, holding_id_v2)
                      left join raw_data_3m using (profile_id, holding_id_v2)
                      left join raw_data_1y using (profile_id, holding_id_v2)
                      left join raw_data_5y using (profile_id, holding_id_v2)
                      left join raw_data_all using (profile_id, holding_id_v2)
         )
select profile_id,
       holding_id_v2,
       symbol,
       collection_id,
       actual_value,
       actual_value - prev_value_1d - cash_flow_sum_1d as absolute_gain_1d,
       actual_value - prev_value_1w - cash_flow_sum_1w as absolute_gain_1w,
       actual_value - prev_value_1m - cash_flow_sum_1m as absolute_gain_1m,
       actual_value - prev_value_3m - cash_flow_sum_3m as absolute_gain_3m,
       actual_value - prev_value_1y - cash_flow_sum_1y as absolute_gain_1y,
       actual_value - prev_value_5y - cash_flow_sum_5y as absolute_gain_5y,
       actual_value - cash_flow_sum_total              as absolute_gain_total,
       relative_gain_1d,
       relative_gain_1w,
       relative_gain_1m,
       relative_gain_3m,
       relative_gain_1y,
       relative_gain_5y,
       relative_gain_total,
       cash_flow_sum_total,
       0::double precision                             as ltt_quantity_total, -- TODO calculate
       now()::timestamp                                as updated_at
from (
         select drivewealth_holdings.profile_id,
                drivewealth_holdings.holding_id_v2,
                drivewealth_holdings.symbol,
                drivewealth_holdings.collection_id,
                drivewealth_holdings.actual_value,
                prev_value_1d                                                            as prev_value_1d,
                prev_value_1w                                                            as prev_value_1w,
                prev_value_1m                                                            as prev_value_1m,
                prev_value_3m                                                            as prev_value_3m,
                prev_value_1y                                                            as prev_value_1y,
                prev_value_5y                                                            as prev_value_5y,
                cash_flow_sum_1d + coalesce(drivewealth_latest_cashflow.cash_flow, 0)    as cash_flow_sum_1d,
                cash_flow_sum_1w + coalesce(drivewealth_latest_cashflow.cash_flow, 0)    as cash_flow_sum_1w,
                cash_flow_sum_1m + coalesce(drivewealth_latest_cashflow.cash_flow, 0)    as cash_flow_sum_1m,
                cash_flow_sum_3m + coalesce(drivewealth_latest_cashflow.cash_flow, 0)    as cash_flow_sum_3m,
                cash_flow_sum_1y + coalesce(drivewealth_latest_cashflow.cash_flow, 0)    as cash_flow_sum_1y,
                cash_flow_sum_5y + coalesce(drivewealth_latest_cashflow.cash_flow, 0)    as cash_flow_sum_5y,
                cash_flow_sum_total + coalesce(drivewealth_latest_cashflow.cash_flow, 0) as cash_flow_sum_total,
                relative_gain_1d,
                relative_gain_1w,
                relative_gain_1m,
                relative_gain_3m,
                relative_gain_1y,
                relative_gain_5y,
                relative_gain_total
         from {{ ref('drivewealth_holdings') }}
                  left join {{ ref('drivewealth_portfolio_historical_holdings_marked') }}
                            on drivewealth_holdings.profile_id = drivewealth_portfolio_historical_holdings_marked.profile_id
                                and drivewealth_holdings.symbol = drivewealth_portfolio_historical_holdings_marked.symbol
                                and ((drivewealth_holdings.collection_id is null and drivewealth_portfolio_historical_holdings_marked.collection_id is null)
                                    or drivewealth_holdings.collection_id = drivewealth_portfolio_historical_holdings_marked.collection_id)
                  left join {{ ref('drivewealth_latest_cashflow') }}
                            on drivewealth_holdings.profile_id = drivewealth_latest_cashflow.profile_id
                                and drivewealth_holdings.symbol = drivewealth_latest_cashflow.symbol
                                and ((drivewealth_holdings.collection_id is null and drivewealth_latest_cashflow.collection_id is null)
                                    or drivewealth_holdings.collection_id = drivewealth_latest_cashflow.collection_id)
                  left join relative_gains
                            on drivewealth_holdings.profile_id = relative_gains.profile_id
                                and drivewealth_holdings.symbol = relative_gains.symbol
                                and ((drivewealth_holdings.collection_id is null and relative_gains.collection_id is null)
                                    or drivewealth_holdings.collection_id = relative_gains.collection_id)
    ) t