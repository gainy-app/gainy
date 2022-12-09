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
                 holding_id_v2
                 ) holding_id_v2,
                   greatest(ticker_realtime_metrics.date,
                            drivewealth_portfolio_historical_holdings.date) as date,
                   case
                       when drivewealth_portfolio_historical_holdings.date < ticker_realtime_metrics.date
                           then value * ticker_realtime_metrics.relative_daily_change
                       else value * drivewealth_portfolio_historical_holdings.relative_daily_gain / (1 + drivewealth_portfolio_historical_holdings.relative_daily_gain)
                       end                                                         as absolute_gain,
                   case
                       when drivewealth_portfolio_historical_holdings.date < ticker_realtime_metrics.date
                           then value * (1 + ticker_realtime_metrics.relative_daily_change)
                       else value
                       end                                                         as actual_value
             from {{ ref('drivewealth_portfolio_historical_holdings') }}
                      join {{ ref('ticker_realtime_metrics') }} using (symbol)
             order by holding_id_v2, drivewealth_portfolio_historical_holdings.date desc
         ),
     raw_data_1w as
         (
             select holding_id_v2,
                    t.absolute_gain + raw_data_1d.absolute_gain as absolute_gain
             from (
                      select holding_id_v2,
                             sum(value * relative_daily_gain / (1 + relative_daily_gain)) as absolute_gain
                      from {{ ref('drivewealth_portfolio_historical_holdings') }}
                               left join raw_data_1d using (holding_id_v2)
                      where drivewealth_portfolio_historical_holdings.date > now() - interval '1 week'
                          and drivewealth_portfolio_historical_holdings.date < raw_data_1d.date or raw_data_1d.date is null
                      group by holding_id_v2
                  ) t
                      left join raw_data_1d using (holding_id_v2)
     )
select holding_id_v2,
       raw_data_1d.actual_value  as actual_value,
       raw_data_1d.absolute_gain as absolute_gain_1d,
       raw_data_1w.absolute_gain as absolute_gain_1w
from raw_data_1d
         left join raw_data_1w using (holding_id_v2)
