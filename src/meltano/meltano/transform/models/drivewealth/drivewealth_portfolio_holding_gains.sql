{{
  config(
    materialized = "incremental",
    unique_key = "holding_id_v2",
    post_hook=[
      pk('holding_id_v2'),
    ]
  )
}}


with raw_data_1m as
         (
             select holding_id_v2,
                    sum(value * relative_daily_gain / (1 + relative_daily_gain)) as absolute_gain
             from {{ ref('drivewealth_portfolio_historical_holdings') }}
             where date > now() - interval '1 month'
             group by holding_id_v2
     ),
     raw_data_3m as
         (
             select holding_id_v2,
                    sum(value * relative_daily_gain / (1 + relative_daily_gain)) as absolute_gain
             from {{ ref('drivewealth_portfolio_historical_holdings') }}
             where date > now() - interval '3 months'
             group by holding_id_v2
     ),
     raw_data_1y as
         (
             select holding_id_v2,
                    sum(value * relative_daily_gain / (1 + relative_daily_gain)) as absolute_gain
             from {{ ref('drivewealth_portfolio_historical_holdings') }}
             where date > now() - interval '1 year'
             group by holding_id_v2
     ),
     raw_data_5y as
         (
             select holding_id_v2,
                    sum(value * relative_daily_gain / (1 + relative_daily_gain)) as absolute_gain
             from {{ ref('drivewealth_portfolio_historical_holdings') }}
             where date > now() - interval '5 years'
             group by holding_id_v2
     ),
     raw_data_total as
         (
             select holding_id_v2,
                    sum(value * relative_daily_gain / (1 + relative_daily_gain)) as absolute_gain
             from {{ ref('drivewealth_portfolio_historical_holdings') }}
             group by holding_id_v2
     )
select holding_id_v2,
       raw_data_1m.absolute_gain    as absolute_gain_1m,
       raw_data_3m.absolute_gain    as absolute_gain_3m,
       raw_data_1y.absolute_gain    as absolute_gain_1y,
       raw_data_5y.absolute_gain    as absolute_gain_5y,
       raw_data_total.absolute_gain as absolute_gain_total
from raw_data_1m
         left join raw_data_3m using (holding_id_v2)
         left join raw_data_1y using (holding_id_v2)
         left join raw_data_5y using (holding_id_v2)
         left join raw_data_total using (holding_id_v2)
