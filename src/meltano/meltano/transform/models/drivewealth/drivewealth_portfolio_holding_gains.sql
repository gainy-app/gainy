{{
  config(
    materialized = "view",
  )
}}

-- HP = EV / (BV + CF) - 1
select profile_id,
       holding_id_v2,
       actual_value,
       actual_value - prev_value_1d - cash_flow_sum_1d as absolute_gain_1d,
       actual_value - prev_value_1w - cash_flow_sum_1w as absolute_gain_1w,
       actual_value - prev_value_1m - cash_flow_sum_1m as absolute_gain_1m,
       actual_value - prev_value_3m - cash_flow_sum_3m as absolute_gain_3m,
       actual_value - prev_value_1y - cash_flow_sum_1y as absolute_gain_1y,
       actual_value - prev_value_5y - cash_flow_sum_5y as absolute_gain_5y,
       actual_value - cash_flow_sum_total              as absolute_gain_total,
       case
           when prev_value_1d > 0
               then (actual_value - cash_flow_sum_1d) / prev_value_1d - 1
           when cash_flow_positive_sum_total > 0
               then (actual_value - cash_flow_negative_sum_total) / cash_flow_positive_sum_total - 1
           end                                         as relative_gain_1d,
       case
           when prev_value_1w > 0
               then (actual_value - cash_flow_sum_1w) / prev_value_1w - 1
           when cash_flow_positive_sum_total > 0
               then (actual_value - cash_flow_negative_sum_total) / cash_flow_positive_sum_total - 1
           end                                         as relative_gain_1w,
       case
           when prev_value_1m > 0
               then (actual_value - cash_flow_sum_1m) / prev_value_1m - 1
           when cash_flow_positive_sum_total > 0
               then (actual_value - cash_flow_negative_sum_total) / cash_flow_positive_sum_total - 1
           end                                         as relative_gain_1m,
       case
           when prev_value_3m > 0
              then (actual_value - cash_flow_sum_3m) / prev_value_3m - 1
           when cash_flow_positive_sum_total > 0
               then (actual_value - cash_flow_negative_sum_total) / cash_flow_positive_sum_total - 1
           end                                         as relative_gain_3m,
       case
           when prev_value_1y > 0
               then (actual_value - cash_flow_sum_1y) / prev_value_1y - 1
           when cash_flow_positive_sum_total > 0
               then (actual_value - cash_flow_negative_sum_total) / cash_flow_positive_sum_total - 1
           end                                         as relative_gain_1y,
       case
           when prev_value_5y > 0
               then (actual_value - cash_flow_sum_5y) / prev_value_5y - 1
           when cash_flow_positive_sum_total > 0
               then (actual_value - cash_flow_negative_sum_total) / cash_flow_positive_sum_total - 1
           end                                         as relative_gain_5y,
       case
           when cash_flow_positive_sum_total > 0
               then (actual_value - cash_flow_negative_sum_total) / cash_flow_positive_sum_total - 1
           end                                         as relative_gain_total,
       0                                               as ltt_quantity_total, -- TODO calculate
       cash_flow_sum_total,
       now()::timestamp                                as updated_at
from (
    select drivewealth_holdings.profile_id,
           drivewealth_holdings.holding_id_v2,
           drivewealth_holdings.symbol,
           drivewealth_holdings.collection_id,
           drivewealth_holdings.actual_value,
           prev_value_1d                                                                           as prev_value_1d,
           prev_value_1w                                                                           as prev_value_1w,
           prev_value_1m                                                                           as prev_value_1m,
           prev_value_3m                                                                           as prev_value_3m,
           prev_value_1y                                                                           as prev_value_1y,
           prev_value_5y                                                                           as prev_value_5y,
           cash_flow_sum_1d + coalesce(drivewealth_latest_cashflow.cash_flow, 0)                   as cash_flow_sum_1d,
           cash_flow_sum_1w + coalesce(drivewealth_latest_cashflow.cash_flow, 0)                   as cash_flow_sum_1w,
           cash_flow_sum_1m + coalesce(drivewealth_latest_cashflow.cash_flow, 0)                   as cash_flow_sum_1m,
           cash_flow_sum_3m + coalesce(drivewealth_latest_cashflow.cash_flow, 0)                   as cash_flow_sum_3m,
           cash_flow_sum_1y + coalesce(drivewealth_latest_cashflow.cash_flow, 0)                   as cash_flow_sum_1y,
           cash_flow_sum_5y + coalesce(drivewealth_latest_cashflow.cash_flow, 0)                   as cash_flow_sum_5y,
           cash_flow_sum_total + coalesce(drivewealth_latest_cashflow.cash_flow, 0)                as cash_flow_sum_total,
           case
               when drivewealth_latest_cashflow.cash_flow is not null and drivewealth_latest_cashflow.cash_flow > 0
                   then drivewealth_latest_cashflow.cash_flow
               else 0
               end + drivewealth_portfolio_historical_holdings_marked.cash_flow_positive_sum_total as cash_flow_positive_sum_total,
           case
               when drivewealth_latest_cashflow.cash_flow is not null and drivewealth_latest_cashflow.cash_flow < 0
                   then drivewealth_latest_cashflow.cash_flow
               else 0
               end + drivewealth_portfolio_historical_holdings_marked.cash_flow_negative_sum_total as cash_flow_negative_sum_total
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
    ) t