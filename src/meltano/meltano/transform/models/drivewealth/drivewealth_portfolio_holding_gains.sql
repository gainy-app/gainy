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
             select profile_id,
                    collection_id,
                    symbol,
                    sum(relative_daily_gain * value) as absolute_gain
             from {{ ref('drivewealth_portfolio_historical_holdings') }}
             where date > now() - interval '1 month'
             group by profile_id, collection_id, symbol
     ),
     raw_data_3m as
         (
             select profile_id,
                    collection_id,
                    symbol,
                    sum(relative_daily_gain * value) as absolute_gain
             from {{ ref('drivewealth_portfolio_historical_holdings') }}
             where date > now() - interval '3 months'
             group by profile_id, collection_id, symbol
     ),
     raw_data_1y as
         (
             select profile_id,
                    collection_id,
                    symbol,
                    sum(relative_daily_gain * value) as absolute_gain
             from {{ ref('drivewealth_portfolio_historical_holdings') }}
             where date > now() - interval '1 year'
             group by profile_id, collection_id, symbol
     ),
     raw_data_5y as
         (
             select profile_id,
                    collection_id,
                    symbol,
                    sum(relative_daily_gain * value) as absolute_gain
             from {{ ref('drivewealth_portfolio_historical_holdings') }}
             where date > now() - interval '5 years'
             group by profile_id, collection_id, symbol
     ),
     raw_data_total as
         (
             select profile_id,
                    collection_id,
                    symbol,
                    sum(relative_daily_gain * value) as absolute_gain
             from {{ ref('drivewealth_portfolio_historical_holdings') }}
             group by profile_id, collection_id, symbol
     )
select holding_id_v2,
       raw_data_1m.absolute_gain    as absolute_gain_1m,
       raw_data_3m.absolute_gain    as absolute_gain_3m,
       raw_data_1y.absolute_gain    as absolute_gain_1y,
       raw_data_5y.absolute_gain    as absolute_gain_5y,
       raw_data_total.absolute_gain as absolute_gain_total
from {{ ref('profile_holdings_normalized') }}
         left join raw_data_1m using (profile_id, collection_id, symbol)
         left join raw_data_3m using (profile_id, collection_id, symbol)
         left join raw_data_1y using (profile_id, collection_id, symbol)
         left join raw_data_5y using (profile_id, collection_id, symbol)
         left join raw_data_total using (profile_id, collection_id, symbol)
where profile_holdings_normalized.collection_id is not null
