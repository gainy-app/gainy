{{
  config(
    materialized = "incremental",
    unique_key = "id",
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
                    max(updated_at) as updated_at
             from {{ ref('drivewealth_portfolio_historical_holdings') }}
             group by profile_id, holding_id_v2, symbol, date_month
     ),

     data as
         (
             select drivewealth_portfolio_historical_holdings.profile_id,
                    drivewealth_portfolio_historical_holdings.holding_id_v2,
                    drivewealth_portfolio_historical_holdings.symbol,
                    '3min'                                              as period,
                    historical_prices_aggregated_3min.datetime,
                    sum(drivewealth_portfolio_historical_holdings.value * historical_prices_aggregated_3min.open /
                        historical_prices_aggregated_1d.adjusted_close) as open,
                    sum(drivewealth_portfolio_historical_holdings.value * historical_prices_aggregated_3min.high /
                        historical_prices_aggregated_1d.adjusted_close) as high,
                    sum(drivewealth_portfolio_historical_holdings.value * historical_prices_aggregated_3min.low /
                        historical_prices_aggregated_1d.adjusted_close) as low,
                    sum(drivewealth_portfolio_historical_holdings.value * historical_prices_aggregated_3min.close /
                        historical_prices_aggregated_1d.adjusted_close) as close,
                    sum(drivewealth_portfolio_historical_holdings.value * historical_prices_aggregated_3min.adjusted_close /
                        historical_prices_aggregated_1d.adjusted_close) as adjusted_close,
                    max(greatest(drivewealth_portfolio_historical_holdings.updated_at,
                        historical_prices_aggregated_1d.updated_at,
                        historical_prices_aggregated_3min.updated_at))  as updated_at
             from {{ ref('drivewealth_portfolio_historical_holdings') }}
                      join {{ ref('historical_prices_aggregated_1d') }}
                           on historical_prices_aggregated_1d.symbol = drivewealth_portfolio_historical_holdings.symbol
                               and historical_prices_aggregated_1d.datetime = drivewealth_portfolio_historical_holdings.date
                      join {{ ref('week_trading_sessions_static') }}
                           on week_trading_sessions_static.symbol = drivewealth_portfolio_historical_holdings.symbol
                               and week_trading_sessions_static.prev_date = drivewealth_portfolio_historical_holdings.date
                      join {{ ref('historical_prices_aggregated_3min') }}
                           on historical_prices_aggregated_3min.symbol = drivewealth_portfolio_historical_holdings.symbol
                               and historical_prices_aggregated_3min.date = week_trading_sessions_static.date
             group by drivewealth_portfolio_historical_holdings.profile_id, drivewealth_portfolio_historical_holdings.holding_id_v2,
                 drivewealth_portfolio_historical_holdings.symbol, historical_prices_aggregated_3min.datetime

             union all

             select drivewealth_portfolio_historical_holdings.profile_id,
                    drivewealth_portfolio_historical_holdings.holding_id_v2,
                    drivewealth_portfolio_historical_holdings.symbol,
                    '15min'                                             as period,
                    historical_prices_aggregated_15min.datetime,
                    sum(drivewealth_portfolio_historical_holdings.value * historical_prices_aggregated_15min.open /
                        historical_prices_aggregated_1d.adjusted_close) as open,
                    sum(drivewealth_portfolio_historical_holdings.value * historical_prices_aggregated_15min.high /
                        historical_prices_aggregated_1d.adjusted_close) as high,
                    sum(drivewealth_portfolio_historical_holdings.value * historical_prices_aggregated_15min.low /
                        historical_prices_aggregated_1d.adjusted_close) as low,
                    sum(drivewealth_portfolio_historical_holdings.value * historical_prices_aggregated_15min.close /
                        historical_prices_aggregated_1d.adjusted_close) as close,
                    sum(drivewealth_portfolio_historical_holdings.value * historical_prices_aggregated_15min.adjusted_close /
                        historical_prices_aggregated_1d.adjusted_close) as adjusted_close,
                    max(greatest(drivewealth_portfolio_historical_holdings.updated_at,
                        historical_prices_aggregated_1d.updated_at,
                        historical_prices_aggregated_15min.updated_at)) as updated_at
             from {{ ref('drivewealth_portfolio_historical_holdings') }}
                      join {{ ref('historical_prices_aggregated_1d') }}
                           on historical_prices_aggregated_1d.symbol = drivewealth_portfolio_historical_holdings.symbol
                               and historical_prices_aggregated_1d.datetime = drivewealth_portfolio_historical_holdings.date
                      join {{ ref('week_trading_sessions_static') }}
                           on week_trading_sessions_static.symbol = drivewealth_portfolio_historical_holdings.symbol
                               and week_trading_sessions_static.prev_date = drivewealth_portfolio_historical_holdings.date
                      join {{ ref('historical_prices_aggregated_15min') }}
                           on historical_prices_aggregated_15min.symbol = drivewealth_portfolio_historical_holdings.symbol
                               and historical_prices_aggregated_15min.date = week_trading_sessions_static.date
             group by drivewealth_portfolio_historical_holdings.profile_id, drivewealth_portfolio_historical_holdings.holding_id_v2,
                 drivewealth_portfolio_historical_holdings.symbol, historical_prices_aggregated_15min.datetime

             union all

             select profile_id,
                    holding_id_v2,
                    symbol,
                    '1d'  as period,
                    date  as datetime,
                    value as open,
                    value as high,
                    value as low,
                    value as close,
                    value as adjusted_close,
                    updated_at
             from {{ ref('drivewealth_portfolio_historical_holdings') }}

             union all

             select data.profile_id,
                    data.holding_id_v2,
                    data.symbol,
                    '1w'            as period,
                    data.date_week  as datetime,
                    dhh_open.value  as open,
                    data.high,
                    data.low,
                    dhh_close.value as close,
                    dhh_close.value as adjusted_close,
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
                    data.date_month as datetime,
                    dhh_open.value  as open,
                    data.high,
                    data.low,
                    dhh_close.value as close,
                    dhh_close.value as adjusted_close,
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
       holding_id_v2                                     as transaction_uniq_id,
       holding_id_v2 || '_' || period || '_' || datetime as id
from data

{% if is_incremental() %}
         left join {{ this }} old_data using (profile_id, holding_id_v2, period, symbol, datetime)
where old_data.profile_id is null
   or data.updated_at > old_data.updated_at
{% endif %}
