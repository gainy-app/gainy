{{
  config(
    materialized = "incremental",
    unique_key = "holding_id_v2",
    tags = ["realtime"],
    post_hook=[
      pk('holding_id_v2'),
    ]
  )
}}

select t.*,
       actual_value * (1 - 1 / (1 + relative_gain_1d)) as absolute_gain_1d,
       actual_value * (1 - 1 / (1 + relative_gain_1w)) as absolute_gain_1w,
       actual_value * (1 - 1 / (1 + relative_gain_1m)) as absolute_gain_1m,
       actual_value * (1 - 1 / (1 + relative_gain_3m)) as absolute_gain_3m,
       actual_value * (1 - 1 / (1 + relative_gain_1y)) as absolute_gain_1y,
       actual_value * (1 - 1 / (1 + relative_gain_5y)) as absolute_gain_5y,
       actual_value * (1 - 1 / (1 + relative_gain_total)) as absolute_gain_total
from (
         select holding_id_v2,
                actual_value,
                -- HP = EV / (BV + CF) - 1
                case
                    when prev_value_1d + cash_flow_sum_1d > 0
                        then actual_value / (prev_value_1d + cash_flow_sum_1d) - 1
                    end as relative_gain_1d,
                case
                    when prev_value_1w + cash_flow_sum_1w > 0
                        then actual_value / (prev_value_1w + cash_flow_sum_1w) - 1
                    end as relative_gain_1w,
                case
                    when prev_value_1m + cash_flow_sum_1m > 0
                        then actual_value / (prev_value_1m + cash_flow_sum_1m) - 1
                    end as relative_gain_1m,
                case
                    when prev_value_3m + cash_flow_sum_3m > 0
                       then actual_value / (prev_value_3m + cash_flow_sum_3m) - 1
                    end as relative_gain_3m,
                case
                    when prev_value_1y + cash_flow_sum_1y > 0
                        then actual_value / (prev_value_1y + cash_flow_sum_1y) - 1
                    end as relative_gain_1y,
                case
                    when prev_value_5y + cash_flow_sum_5y > 0
                        then actual_value / (prev_value_5y + cash_flow_sum_5y) - 1
                    end as relative_gain_5y,
                case
                    when cash_flow_sum_total > 0
                        then actual_value / cash_flow_sum_total - 1
                    end as relative_gain_total,
                drivewealth_portfolio_historical_holdings_marked.updated_at
         from {{ ref('drivewealth_holdings') }}
                  left join {{ ref('drivewealth_portfolio_historical_holdings_marked') }}
                            on drivewealth_holdings.profile_id = drivewealth_portfolio_historical_holdings_marked.profile_id
                                and drivewealth_holdings.symbol = drivewealth_portfolio_historical_holdings_marked.symbol
                                and ((drivewealth_holdings.collection_id is null and drivewealth_portfolio_historical_holdings_marked.collection_id is null)
                                    or drivewealth_holdings.collection_id = drivewealth_portfolio_historical_holdings_marked.collection_id)
     ) t
