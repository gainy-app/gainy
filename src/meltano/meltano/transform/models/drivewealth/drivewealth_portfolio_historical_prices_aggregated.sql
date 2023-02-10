{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('profile_id, holding_id_v2, period, symbol, datetime'),
      index('id', true),
    ]
  )
}}

    
with chart_1w as
         (
             select profile_id,
                    holding_id_v2,
                    symbol,
                    date_week,
                    min(date)       as open_date,
                    max(date)       as close_date,
                    max(value)      as high,
                    min(value)      as low,
                    exp(sum(ln(coalesce(relative_daily_gain, 0) + 1 + 1e-10))) - 1 as relative_gain,
                    max(updated_at) as updated_at
             from {{ ref('drivewealth_portfolio_historical_holdings') }}
             group by profile_id, holding_id_v2, symbol, date_week
     ),

     chart_1m as
         (
             select profile_id,
                    holding_id_v2,
                    symbol,
                    date_month,
                    min(date)       as open_date,
                    max(date)       as close_date,
                    max(value)      as high,
                    min(value)      as low,
                    exp(sum(ln(coalesce(relative_daily_gain, 0) + 1 + 1e-10))) - 1 as relative_gain,
                    max(updated_at) as updated_at
             from {{ ref('drivewealth_portfolio_historical_holdings') }}
             group by profile_id, holding_id_v2, symbol, date_month
     ),

     data as
         (
             select data.profile_id,
                    data.holding_id_v2,
                    data.symbol,
                    '3min'     as period,
                    data.date,
                    data.datetime,
                    data.value as open,
                    data.value as high,
                    data.value as low,
                    data.value as close,
                    data.value as adjusted_close,
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

             union all

             select data.profile_id,
                    data.holding_id_v2,
                    data.symbol,
                    '15min'    as period,
                    data.date,
                    data.datetime,
                    data.value as open,
                    data.value as high,
                    data.value as low,
                    data.value as close,
                    data.value as adjusted_close,
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

             union all

             select data.profile_id,
                    data.holding_id_v2,
                    data.symbol,
                    '1d'  as period,
                    data.date,
                    data.date  as datetime,
                    data.value as open,
                    data.value as high,
                    data.value as low,
                    data.value as close,
                    data.value as adjusted_close,
                    data.relative_daily_gain as relative_gain,
                    data.updated_at
             from {{ ref('drivewealth_portfolio_historical_holdings') }} data

             union all

             select data.profile_id,
                    data.holding_id_v2,
                    data.symbol,
                    '1w'            as period,
                    data.date_week  as date,
                    data.date_week  as datetime,
                    dhh_open.value  as open,
                    data.high,
                    data.low,
                    dhh_close.value as close,
                    dhh_close.value as adjusted_close,
                    data.relative_gain,
                    data.updated_at
             from chart_1w data
                      join {{ ref('drivewealth_portfolio_historical_holdings') }} dhh_open
                           on dhh_open.profile_id = data.profile_id
                               and dhh_open.holding_id_v2 = data.holding_id_v2
                               and dhh_open.symbol = data.symbol
                               and dhh_open.date = data.open_date
                      join {{ ref('drivewealth_portfolio_historical_holdings') }} dhh_close
                           on dhh_close.profile_id = data.profile_id
                               and dhh_close.holding_id_v2 = data.holding_id_v2
                               and dhh_close.symbol = data.symbol
                               and dhh_close.date = data.close_date

             union all

             select data.profile_id,
                    data.holding_id_v2,
                    data.symbol,
                    '1m'            as period,
                    data.date_month as date,
                    data.date_month as datetime,
                    dhh_open.value  as open,
                    data.high,
                    data.low,
                    dhh_close.value as close,
                    dhh_close.value as adjusted_close,
                    data.relative_gain,
                    data.updated_at
             from chart_1m data
                      join {{ ref('drivewealth_portfolio_historical_holdings') }} dhh_open
                           on dhh_open.profile_id = data.profile_id
                               and dhh_open.holding_id_v2 = data.holding_id_v2
                               and dhh_open.symbol = data.symbol
                               and dhh_open.date = data.open_date
                      join {{ ref('drivewealth_portfolio_historical_holdings') }} dhh_close
                           on dhh_close.profile_id = data.profile_id
                               and dhh_close.holding_id_v2 = data.holding_id_v2
                               and dhh_close.symbol = data.symbol
                               and dhh_close.date = data.close_date
     )

select data.*,
       holding_id_v2 || '_' || period || '_' || datetime as id
from data

{% if is_incremental() %}
         left join {{ this }} old_data using (profile_id, holding_id_v2, period, symbol, datetime)
where old_data.profile_id is null
   or data.updated_at > old_data.updated_at
{% endif %}
