{{
  config(
    materialized = "view",
  )
}}

-- HP = EV / (BV + CF) - 1
select drivewealth_holdings.profile_id,
       drivewealth_holdings.holding_id_v2,
       actual_value,
       case
           when coalesce(prev_value_1d, 0) + (coalesce(cash_flow_sum_1d, 0) + coalesce(drivewealth_latest_cashflow.cash_flow, 0)) > 0
               then actual_value / (prev_value_1d + (coalesce(cash_flow_sum_1d, 0) + coalesce(drivewealth_latest_cashflow.cash_flow, 0))) - 1
           end                                                                                  as relative_gain_1d,
       actual_value - coalesce(prev_value_1d, 0) -
           (coalesce(cash_flow_sum_1d, 0) + coalesce(drivewealth_latest_cashflow.cash_flow, 0)) as absolute_gain_1d,
       case
           when coalesce(prev_value_1w, 0) + (coalesce(cash_flow_sum_1w, 0) + coalesce(drivewealth_latest_cashflow.cash_flow, 0)) > 0
               then actual_value / (prev_value_1w + (coalesce(cash_flow_sum_1w, 0) + coalesce(drivewealth_latest_cashflow.cash_flow, 0))) - 1
           end                                                                                  as relative_gain_1w,
       actual_value - coalesce(prev_value_1w, 0) -
           (coalesce(cash_flow_sum_1w, 0) + coalesce(drivewealth_latest_cashflow.cash_flow, 0)) as absolute_gain_1w,
       case
           when coalesce(prev_value_1m, 0) + (coalesce(cash_flow_sum_1m, 0) + coalesce(drivewealth_latest_cashflow.cash_flow, 0)) > 0
               then actual_value / (prev_value_1m + (coalesce(cash_flow_sum_1m, 0) + coalesce(drivewealth_latest_cashflow.cash_flow, 0))) - 1
           end                                                                                  as relative_gain_1m,
       actual_value - coalesce(prev_value_1m, 0) -
           (coalesce(cash_flow_sum_1m, 0) + coalesce(drivewealth_latest_cashflow.cash_flow, 0)) as absolute_gain_1m,
       case
           when coalesce(prev_value_3m, 0) + (coalesce(cash_flow_sum_3m, 0) + coalesce(drivewealth_latest_cashflow.cash_flow, 0)) > 0
              then actual_value / (prev_value_3m + (coalesce(cash_flow_sum_3m, 0) + coalesce(drivewealth_latest_cashflow.cash_flow, 0))) - 1
           end                                                                                  as relative_gain_3m,
       actual_value - coalesce(prev_value_3m, 0) -
           (coalesce(cash_flow_sum_3m, 0) + coalesce(drivewealth_latest_cashflow.cash_flow, 0)) as absolute_gain_3m,
       case
           when coalesce(prev_value_1y, 0) + (coalesce(cash_flow_sum_1y, 0) + coalesce(drivewealth_latest_cashflow.cash_flow, 0)) > 0
               then actual_value / (prev_value_1y + (coalesce(cash_flow_sum_1y, 0) + coalesce(drivewealth_latest_cashflow.cash_flow, 0))) - 1
           end                                                                                  as relative_gain_1y,
       actual_value - coalesce(prev_value_1y, 0) -
           (coalesce(cash_flow_sum_1y, 0) + coalesce(drivewealth_latest_cashflow.cash_flow, 0)) as absolute_gain_1y,
       case
           when coalesce(prev_value_5y, 0) + (coalesce(cash_flow_sum_5y, 0) + coalesce(drivewealth_latest_cashflow.cash_flow, 0)) > 0
               then actual_value / (prev_value_5y + (coalesce(cash_flow_sum_5y, 0) + coalesce(drivewealth_latest_cashflow.cash_flow, 0))) - 1
           end                                                                                  as relative_gain_5y,
       actual_value - coalesce(prev_value_5y, 0) -
           (coalesce(cash_flow_sum_5y, 0) + coalesce(drivewealth_latest_cashflow.cash_flow, 0)) as absolute_gain_5y,
       case
           when (coalesce(cash_flow_sum_total, 0) + coalesce(drivewealth_latest_cashflow.cash_flow, 0)) > 0
               then actual_value / (coalesce(cash_flow_sum_total, 0) + coalesce(drivewealth_latest_cashflow.cash_flow, 0)) - 1
           end                                                                                  as relative_gain_total,
       actual_value - (coalesce(cash_flow_sum_total, 0) +
                       coalesce(drivewealth_latest_cashflow.cash_flow, 0))                      as absolute_gain_total,
       0                                                                                        as ltt_quantity_total, -- TODO calculate
       drivewealth_portfolio_historical_holdings_marked.updated_at
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
