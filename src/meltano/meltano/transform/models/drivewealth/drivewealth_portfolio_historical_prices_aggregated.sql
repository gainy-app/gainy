{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('holding_id_v2, period, datetime'),
      index('id', true),
    ]
  )
}}

    
with chart_1w as
         (
             select profile_id,
                    holding_id_v2,
                    date_week,
                    min(date)       as open_date,
                    max(date)       as close_date,
                    max(value)      as high,
                    min(value)      as low,
                    exp(sum(ln(coalesce(relative_daily_gain, 0) + 1 + 1e-10))) - 1 as relative_gain,
                    max(updated_at) as updated_at
             from {{ ref('drivewealth_portfolio_historical_holdings') }}
             group by profile_id, holding_id_v2, date_week
     ),

     chart_1m as
         (
             select profile_id,
                    holding_id_v2,
                    date_month,
                    min(date)       as open_date,
                    max(date)       as close_date,
                    max(value)      as high,
                    min(value)      as low,
                    exp(sum(ln(coalesce(relative_daily_gain, 0) + 1 + 1e-10))) - 1 as relative_gain,
                    max(updated_at) as updated_at
             from {{ ref('drivewealth_portfolio_historical_holdings') }}
             group by profile_id, holding_id_v2, date_month
     ),

     data as
         (
             select data.profile_id,
                    data.holding_id_v2,
                    '3min' as period,
                    data.date,
                    data.datetime,
                    data.value,
                    data.relative_gain,
                    data.updated_at
             from {{ ref('drivewealth_portfolio_historical_prices_aggregated_3min') }} data
                      join {{ ref('week_trading_sessions_static') }}
                           on week_trading_sessions_static.date = data.date
                               and week_trading_sessions_static.symbol = case
                                                                             when data.symbol like 'CUR:%'
                                                                                 then 'SPY'
                                                                             else data.symbol end
             where data.datetime between week_trading_sessions_static.open_at and week_trading_sessions_static.close_at - interval '1 microsecond'
               and week_trading_sessions_static.index >= 0

             union all

             select data.profile_id,
                    data.holding_id_v2,
                    '15min' as period,
                    data.date,
                    data.datetime,
                    data.value,
                    data.relative_gain,
                    data.updated_at
             from {{ ref('drivewealth_portfolio_historical_prices_aggregated_15min') }} data
                      join {{ ref('week_trading_sessions_static') }}
                           on week_trading_sessions_static.date = data.date
                               and week_trading_sessions_static.symbol = case
                                                                             when data.symbol like 'CUR:%'
                                                                                 then 'SPY'
                                                                             else data.symbol end
             where data.datetime between week_trading_sessions_static.open_at and week_trading_sessions_static.close_at - interval '1 microsecond'
               and week_trading_sessions_static.index >= 0

             union all

             select data.profile_id,
                    data.holding_id_v2,
                    '1d'       as period,
                    data.date,
                    data.date  as datetime,
                    data.value,
                    data.relative_daily_gain as relative_gain,
                    data.updated_at
             from {{ ref('drivewealth_portfolio_historical_holdings') }} data

             union all

             select data.profile_id,
                    data.holding_id_v2,
                    '1w'           as period,
                    data.date_week as date,
                    data.date_week as datetime,
                    dhh_close.value,
                    data.relative_gain,
                    data.updated_at
             from chart_1w data
                      join {{ ref('drivewealth_portfolio_historical_holdings') }} dhh_close
                           on dhh_close.profile_id = data.profile_id
                               and dhh_close.holding_id_v2 = data.holding_id_v2
                               and dhh_close.date = data.close_date

             union all

             select data.profile_id,
                    data.holding_id_v2,
                    '1m'            as period,
                    data.date_month as date,
                    data.date_month as datetime,
                    dhh_close.value,
                    data.relative_gain,
                    data.updated_at
             from chart_1m data
                      join {{ ref('drivewealth_portfolio_historical_holdings') }} dhh_close
                           on dhh_close.profile_id = data.profile_id
                               and dhh_close.holding_id_v2 = data.holding_id_v2
                               and dhh_close.date = data.close_date
     ),
     latest_3min_data as
         (
             select t.*,
                    data.date,
                    data.value,
                    data.updated_at
             from (
                      select holding_id_v2,
                             max(datetime)                                            as datetime,
                             exp(sum(ln(coalesce(relative_gain, 0) + 1 + 1e-10))) - 1 as relative_gain
                      from {{ ref('drivewealth_portfolio_historical_prices_aggregated_3min') }} data
                               join (
                                        select holding_id_v2, max(updated_at) as max_updated_at
                                        from {{ ref('drivewealth_portfolio_historical_holdings') }}
                                        group by holding_id_v2
                                    ) t using (holding_id_v2)
                      where data.datetime > max_updated_at
                      group by holding_id_v2
                  ) t
                      join {{ ref('drivewealth_portfolio_historical_prices_aggregated_3min') }} data using (holding_id_v2, datetime)
     ),
     latest_data as
         (
             select holding_id_v2,
                    '15min'                                                                         as period,
                    (date_trunc('minute', datetime) -
                     interval '1 minute' * mod(extract(minutes from datetime)::int, 15))::timestamp as datetime,
                    relative_gain,
                    value,
                    updated_at
             from latest_3min_data

             union all

             select holding_id_v2,
                    '1d'            as period,
                    date::timestamp as datetime,
                    relative_gain,
                    value,
                    updated_at
             from latest_3min_data

             union all

             select holding_id_v2,
                    '1w'                                      as period,
                    date_trunc('week', date::date)::timestamp as datetime,
                    relative_gain,
                    value,
                    updated_at
             from latest_3min_data

             union all

             select holding_id_v2,
                    '1m'                                       as period,
                    date_trunc('month', date::date)::timestamp as datetime,
                    relative_gain,
                    value,
                    updated_at
             from latest_3min_data
     )
select profile_id,
       holding_id_v2,
       period,
       data.date,
       datetime,
       coalesce(latest_data.value, data.value)           as value,
       coalesce((1 + data.relative_gain) * (1 + latest_data.relative_gain) - 1,
                data.relative_gain)                      as relative_gain,
       coalesce(latest_data.updated_at, data.updated_at) as updated_at,
       holding_id_v2 || '_' || period || '_' || datetime as id
from data
         left join latest_data using (holding_id_v2, period, datetime)

{% if is_incremental() %}
         left join {{ this }} old_data using (profile_id, holding_id_v2, period, datetime)
where old_data.profile_id is null
   or abs(data.relative_gain - old_data.relative_gain) > 1e-3
   or abs(data.value - old_data.value) > 1e-3
{% endif %}
