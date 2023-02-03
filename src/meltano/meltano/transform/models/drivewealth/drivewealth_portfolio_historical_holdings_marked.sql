{{
  config(
    materialized = "incremental",
    unique_key = "holding_id_v2",
    tags = ["realtime"],
    post_hook=[
      pk('holding_id_v2'),
      index(['profile_id', 'collection_id', 'symbol'], true),
    ]
  )
}}

with raw_data_1d as
         (
             select holding_id_v2,
                    date       as date_1d,
                    prev_value as prev_value_1d,
                    cash_flow  as cash_flow_sum_1d,
                    drivewealth_portfolio_historical_holdings.updated_at
             from (
                      select profile_id,
                             holding_id_v2,
                             symbol,
                             max(date) as date
                      from {{ ref('drivewealth_portfolio_historical_holdings') }}
                      where date > now()::date - interval '1 week'
                      group by profile_id, holding_id_v2, symbol
                  ) t
                 join {{ ref('drivewealth_portfolio_historical_holdings') }} using (profile_id, holding_id_v2, symbol, date)
     ),
     raw_data_1w as
         (
             select holding_id_v2,
                    date          as date_1w,
                    prev_value    as prev_value_1w,
                    cash_flow_sum as cash_flow_sum_1w
             from (
                      select profile_id,
                             holding_id_v2,
                             symbol,
                             min(date)      as date,
                             sum(cash_flow) as cash_flow_sum
                      from {{ ref('drivewealth_portfolio_historical_holdings') }}
                      where date > now()::date - interval '1 week'
                      group by profile_id, holding_id_v2, symbol
                  ) t
                 join {{ ref('drivewealth_portfolio_historical_holdings') }} using (profile_id, holding_id_v2, symbol, date)
     ),
     raw_data_1m as
         (
             select holding_id_v2,
                    date          as date_1m,
                    prev_value    as prev_value_1m,
                    cash_flow_sum as cash_flow_sum_1m
             from (
                      select profile_id,
                             holding_id_v2,
                             symbol,
                             min(date)      as date,
                             sum(cash_flow) as cash_flow_sum
                      from {{ ref('drivewealth_portfolio_historical_holdings') }}
                      where date > now()::date - interval '1 month'
                      group by profile_id, holding_id_v2, symbol
                  ) t
                 join {{ ref('drivewealth_portfolio_historical_holdings') }} using (profile_id, holding_id_v2, symbol, date)
     ),
     raw_data_3m as
         (
             select holding_id_v2,
                    date          as date_3m,
                    prev_value    as prev_value_3m,
                    cash_flow_sum as cash_flow_sum_3m
             from (
                      select profile_id,
                             holding_id_v2,
                             symbol,
                             min(date)      as date,
                             sum(cash_flow) as cash_flow_sum
                      from {{ ref('drivewealth_portfolio_historical_holdings') }}
                      where date > now()::date - interval '3 month'
                      group by profile_id, holding_id_v2, symbol
                  ) t
                 join {{ ref('drivewealth_portfolio_historical_holdings') }} using (profile_id, holding_id_v2, symbol, date)
     ),
     raw_data_1y as
         (
             select holding_id_v2,
                    date          as date_1y,
                    prev_value    as prev_value_1y,
                    cash_flow_sum as cash_flow_sum_1y
             from (
                      select profile_id,
                             holding_id_v2,
                             symbol,
                             min(date)      as date,
                             sum(cash_flow) as cash_flow_sum
                      from {{ ref('drivewealth_portfolio_historical_holdings') }}
                      where date > now()::date - interval '1 year'
                      group by profile_id, holding_id_v2, symbol
                  ) t
                 join {{ ref('drivewealth_portfolio_historical_holdings') }} using (profile_id, holding_id_v2, symbol, date)
     ),
     raw_data_5y as
         (
             select holding_id_v2,
                    date          as date_5y,
                    prev_value    as prev_value_5y,
                    cash_flow_sum as cash_flow_sum_5y
             from (
                      select profile_id,
                             holding_id_v2,
                             symbol,
                             min(date)      as date,
                             sum(cash_flow) as cash_flow_sum
                      from {{ ref('drivewealth_portfolio_historical_holdings') }}
                      where date > now()::date - interval '5 years'
                      group by profile_id, holding_id_v2, symbol
                  ) t
                 join {{ ref('drivewealth_portfolio_historical_holdings') }} using (profile_id, holding_id_v2, symbol, date)
     ),
     raw_data_all as
         (
             select profile_id,
                    collection_id,
                    holding_id_v2,
                    symbol,
                    date          as date_total,
                    cash_flow_sum as cash_flow_sum_total
             from (
                      select profile_id,
                             holding_id_v2,
                             symbol,
                             min(date)      as date,
                             sum(cash_flow) as cash_flow_sum
                      from {{ ref('drivewealth_portfolio_historical_holdings') }}
                      group by profile_id, holding_id_v2, symbol
                  ) t
                 join {{ ref('drivewealth_portfolio_historical_holdings') }} using (profile_id, holding_id_v2, symbol, date)
     )
select profile_id, holding_id_v2, collection_id, symbol,
       date_1d,
       prev_value_1d,
       cash_flow_sum_1d,
       date_1w,
       prev_value_1w,
       cash_flow_sum_1w,
       date_1m,
       prev_value_1m,
       cash_flow_sum_1m,
       date_3m,
       prev_value_3m,
       cash_flow_sum_3m,
       date_1y,
       prev_value_1y,
       cash_flow_sum_1y,
       date_5y,
       prev_value_5y,
       cash_flow_sum_5y,
       date_total,
       cash_flow_sum_total,
       updated_at
from raw_data_all
         left join raw_data_1d using (holding_id_v2)
         left join raw_data_1w using (holding_id_v2)
         left join raw_data_1m using (holding_id_v2)
         left join raw_data_3m using (holding_id_v2)
         left join raw_data_1y using (holding_id_v2)
         left join raw_data_5y using (holding_id_v2)
