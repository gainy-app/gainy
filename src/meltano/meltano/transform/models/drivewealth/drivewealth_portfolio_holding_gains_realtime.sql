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


with raw_data_1d as
         (
             select distinct on (
                 profile_id, collection_id
                 ) profile_id,
                   collection_id,
                   symbol,
                   greatest(ticker_realtime_metrics.date,
                            drivewealth_portfolio_historical_holdings.date) as date,
                   case
                       when drivewealth_portfolio_historical_holdings.date < ticker_realtime_metrics.date
                           then value * ticker_realtime_metrics.relative_daily_change
                       else value * drivewealth_portfolio_historical_holdings.relative_daily_gain
                       end                                                         as absolute_gain,
                   case
                       when drivewealth_portfolio_historical_holdings.date < ticker_realtime_metrics.date
                           then value * (1 + ticker_realtime_metrics.relative_daily_change)
                       else value
                       end                                                         as actual_value
             from {{ ref('drivewealth_portfolio_historical_holdings') }}
                      join {{ ref('ticker_realtime_metrics') }} using (symbol)
             order by profile_id, collection_id, drivewealth_portfolio_historical_holdings.date desc
         ),
     raw_data_1w as
         (
             select profile_id,
                    collection_id,
                    t.absolute_gain + raw_data_1d.absolute_gain as absolute_gain
             from (
                      select profile_id,
                             collection_id,
                             sum(value *
                                 drivewealth_portfolio_historical_holdings.relative_daily_gain) as absolute_gain
                      from {{ ref('drivewealth_portfolio_historical_holdings') }}
                               left join raw_data_1d using (profile_id, collection_id)
                      where drivewealth_portfolio_historical_holdings.date > now() - interval '1 week'
                          and drivewealth_portfolio_historical_holdings.date < raw_data_1d.date or raw_data_1d.date is null
                      group by profile_id, collection_id
                  ) t
                      left join raw_data_1d using (profile_id, collection_id)
     )
select holding_id_v2,
       raw_data_1d.actual_value  as actual_value,
       raw_data_1d.absolute_gain as absolute_gain_1d,
       raw_data_1w.absolute_gain as absolute_gain_1w
from {{ ref('profile_holdings_normalized') }}
         left join raw_data_1d using (profile_id, collection_id)
         left join raw_data_1w using (profile_id, collection_id)
where profile_holdings_normalized.collection_id is not null
