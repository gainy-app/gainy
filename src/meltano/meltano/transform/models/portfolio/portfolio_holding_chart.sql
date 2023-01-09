{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('id'),
      index(['profile_id', 'holding_id_v2', 'period', 'datetime'], true),
    ],
  )
}}


-- Execution Time: 220791.001 ms
with data as
         (
             select t.*
             from (
                      select profile_holdings_normalized_all.profile_id,
                             portfolio_holding_chart_3min.holding_id_v2,
                             week_trading_sessions_static.date,
                             portfolio_holding_chart_3min.datetime,
                             '1d'::varchar as period,
                             portfolio_holding_chart_3min.open,
                             portfolio_holding_chart_3min.high,
                             portfolio_holding_chart_3min.low,
                             portfolio_holding_chart_3min.close,
                             portfolio_holding_chart_3min.adjusted_close,
                             portfolio_holding_chart_3min.quantity,
                             portfolio_holding_chart_3min.transaction_count,
                             portfolio_holding_chart_3min.relative_gain,
                             portfolio_holding_chart_3min.updated_at
                      from {{ ref('portfolio_holding_chart_3min') }}
                               join {{ ref('profile_holdings_normalized_all') }} using (holding_id_v2)
                               join {{ ref('week_trading_sessions_static') }} using (symbol)
                      where portfolio_holding_chart_3min.datetime between week_trading_sessions_static.open_at and week_trading_sessions_static.close_at - interval '1 microsecond'
                        and not is_app_trading

                      union all

                      select profile_holdings_normalized_all.profile_id,
                             portfolio_holding_chart_15min.holding_id_v2,
                             week_trading_sessions_static.date,
                             portfolio_holding_chart_15min.datetime,
                             '1w'::varchar as period,
                             portfolio_holding_chart_15min.open,
                             portfolio_holding_chart_15min.high,
                             portfolio_holding_chart_15min.low,
                             portfolio_holding_chart_15min.close,
                             portfolio_holding_chart_15min.adjusted_close,
                             portfolio_holding_chart_15min.quantity,
                             portfolio_holding_chart_15min.transaction_count,
                             portfolio_holding_chart_15min.relative_gain,
                             portfolio_holding_chart_15min.updated_at
                      from {{ ref('portfolio_holding_chart_15min') }}
                               join {{ ref('profile_holdings_normalized_all') }} using (holding_id_v2)
                               join {{ ref('week_trading_sessions_static') }} using (symbol)
                      where portfolio_holding_chart_15min.datetime between week_trading_sessions_static.open_at and week_trading_sessions_static.close_at - interval '1 microsecond'
                        and not is_app_trading

                      union all

                      select profile_holdings_normalized_all.profile_id,
                             portfolio_holding_chart_1d.holding_id_v2,
                             portfolio_holding_chart_1d.date,
                             portfolio_holding_chart_1d.date::timestamp as datetime,
                             '1m'::varchar as period,
                             portfolio_holding_chart_1d.open,
                             portfolio_holding_chart_1d.high,
                             portfolio_holding_chart_1d.low,
                             portfolio_holding_chart_1d.close,
                             portfolio_holding_chart_1d.adjusted_close,
                             portfolio_holding_chart_1d.quantity,
                             portfolio_holding_chart_1d.transaction_count,
                             portfolio_holding_chart_1d.relative_gain,
                             portfolio_holding_chart_1d.updated_at
                      from {{ ref('portfolio_holding_chart_1d') }}
                               join {{ ref('profile_holdings_normalized_all') }} using (holding_id_v2)
                      where portfolio_holding_chart_1d.date >= now() - interval '1 month + 1 week'
                        and not is_app_trading

                      union all

                      select profile_holdings_normalized_all.profile_id,
                             portfolio_holding_chart_1d.holding_id_v2,
                             portfolio_holding_chart_1d.date,
                             portfolio_holding_chart_1d.date::timestamp as datetime,
                             '3m'::varchar as period,
                             portfolio_holding_chart_1d.open,
                             portfolio_holding_chart_1d.high,
                             portfolio_holding_chart_1d.low,
                             portfolio_holding_chart_1d.close,
                             portfolio_holding_chart_1d.adjusted_close,
                             portfolio_holding_chart_1d.quantity,
                             portfolio_holding_chart_1d.transaction_count,
                             portfolio_holding_chart_1d.relative_gain,
                             portfolio_holding_chart_1d.updated_at
                      from {{ ref('portfolio_holding_chart_1d') }}
                               join {{ ref('profile_holdings_normalized_all') }} using (holding_id_v2)
                      where portfolio_holding_chart_1d.date >= now() - interval '3 month + 1 week'
                        and not is_app_trading

                      union all

                      select profile_holdings_normalized_all.profile_id,
                             portfolio_holding_chart_1d.holding_id_v2,
                             portfolio_holding_chart_1d.date,
                             portfolio_holding_chart_1d.date::timestamp as datetime,
                             '1y'::varchar as period,
                             portfolio_holding_chart_1d.open,
                             portfolio_holding_chart_1d.high,
                             portfolio_holding_chart_1d.low,
                             portfolio_holding_chart_1d.close,
                             portfolio_holding_chart_1d.adjusted_close,
                             portfolio_holding_chart_1d.quantity,
                             portfolio_holding_chart_1d.transaction_count,
                             portfolio_holding_chart_1d.relative_gain,
                             portfolio_holding_chart_1d.updated_at
                      from {{ ref('portfolio_holding_chart_1d') }}
                               join {{ ref('profile_holdings_normalized_all') }} using (holding_id_v2)
                      where not is_app_trading

                      union all

                      select profile_holdings_normalized_all.profile_id,
                             portfolio_holding_chart_1w.holding_id_v2,
                             portfolio_holding_chart_1w.date,
                             portfolio_holding_chart_1w.date::timestamp as datetime,
                             '5y'::varchar as period,
                             portfolio_holding_chart_1w.open,
                             portfolio_holding_chart_1w.high,
                             portfolio_holding_chart_1w.low,
                             portfolio_holding_chart_1w.close,
                             portfolio_holding_chart_1w.adjusted_close,
                             portfolio_holding_chart_1w.quantity,
                             portfolio_holding_chart_1w.transaction_count,
                             portfolio_holding_chart_1w.relative_gain,
                             portfolio_holding_chart_1w.updated_at
                      from {{ ref('portfolio_holding_chart_1w') }}
                               join {{ ref('profile_holdings_normalized_all') }} using (holding_id_v2)
                      where not is_app_trading

                      union all

                      select profile_id,
                             holding_id_v2,
                             date,
                             date::timestamp as datetime,
                             'all'::varchar  as period,
                             open,
                             high,
                             low,
                             close,
                             adjusted_close,
                             portfolio_holding_chart_1m.quantity,
                             portfolio_holding_chart_1m.transaction_count,
                             portfolio_holding_chart_1m.relative_gain,
                             portfolio_holding_chart_1m.updated_at
                      from {{ ref('portfolio_holding_chart_1m') }}
                               join {{ ref('profile_holdings_normalized_all') }} using (holding_id_v2)
                      where not is_app_trading

                      union all

                      select profile_id,
                             holding_id_v2,
                             date::date,
                             datetime::timestamp,
                             period::varchar,
                             open,
                             high,
                             low,
                             close,
                             adjusted_close,
                             null as quantity,
                             null as transaction_count,
                             relative_gain,
                             updated_at
                      from {{ ref('drivewealth_portfolio_chart') }}
                  ) t
         )
select data.*,
       case
           when data.quantity > 0
               then data.adjusted_close /
                    data.quantity *
                    (last_value(data.quantity)
                        over (partition by profile_id, holding_id_v2, period order by datetime rows between current row and unbounded following) -
                     data.quantity)
           else 0
           end as cash_adjustment,
       profile_id || '_' || holding_id_v2 || '_' || period || '_' || datetime as id
from data

{% if is_incremental() %}
         left join {{ this }} old_data using (profile_id, holding_id_v2, period, datetime)
where old_data.adjusted_close is null
   or (data.relative_gain is not null and old_data.relative_gain is null)
   or abs(data.adjusted_close - old_data.adjusted_close) > 1e-3
{% endif %}
