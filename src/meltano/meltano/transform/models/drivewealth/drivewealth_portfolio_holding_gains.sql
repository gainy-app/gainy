{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('profile_id, holding_id_v2, symbol, date'),
      index('id', true),
      'create index if not exists "dphh_profile_id_holding_id_v2_symbol_date_week" ON {{ this }} (profile_id, holding_id_v2, symbol, date_week)',
      'create index if not exists "dphh_profile_id_holding_id_v2_symbol_date_month" ON {{ this }} (profile_id, holding_id_v2, symbol, date_month)',
    ]
  )
}}

with daily_latest_chart_point as
         (
             select drivewealth_portfolio_historical_prices_aggregated.*,
                    row_number() over (partition by profile_id, holding_id_v2 order by datetime desc) as idx
             from (
                      select profile_id,
                             holding_id_v2,
                             period,
                             symbol,
                             datetime::date as date,
                             max(datetime)  as datetime
                      from drivewealth_portfolio_historical_prices_aggregated
                      where period = '1w'
                      group by profile_id, holding_id_v2, period, symbol, datetime::date
                  ) t
                      join drivewealth_portfolio_historical_prices_aggregated
                           using (profile_id, holding_id_v2, period, symbol, datetime)
     ),
     metrics as
         (
             select profile_id,
                    holding_id_v2,
                    symbol,
                    value_0d /
                    case
                        when coalesce(value_1w, value_all) > 0
                            then coalesce(value_1w, value_all)
                        end - 1 as value_change_1w,
                    value_0d /
                    case
                        when coalesce(value_1m, value_all) > 0
                            then coalesce(value_1m, value_all)
                        end - 1 as value_change_1m,
                    value_0d /
                    case
                        when coalesce(value_3m, value_all) > 0
                            then coalesce(value_3m, value_all)
                        end - 1 as value_change_3m,
                    value_0d /
                    case
                        when coalesce(value_1y, value_all) > 0
                            then coalesce(value_1y, value_all)
                        end - 1 as value_change_1y,
                    value_0d /
                    case
                        when coalesce(value_5y, value_all) > 0
                            then coalesce(value_5y, value_all)
                        end - 1 as value_change_5y,
                    value_0d /
                    case
                        when value_all > 0
                            then value_all
                        end - 1 as value_change_all,
                    updated_at
             from drivewealth_portfolio_historical_holdings_marked
     ),
     data as
         (
             select profile_holdings_normalized.profile_id,
                    profile_holdings_normalized.holding_id_v2,
                    profile_holdings_normalized.symbol,
                    (latest_day.adjusted_close - previous_day.adjusted_close)::double precision as absolute_daily_change,
                    (latest_day.adjusted_close /
                     case when previous_day.adjusted_close > 0 then previous_day.adjusted_close end - 1
                        )::double precision                                                     as relative_daily_change,
                    metrics.value_change_1w,
                    metrics.value_change_1m,
                    metrics.value_change_3m,
                    metrics.value_change_1y,
                    metrics.value_change_5y,
                    metrics.value_change_all
             from profile_holdings_normalized
                      left join metrics using (profile_id, holding_id_v2, symbol)
                      left join daily_latest_chart_point latest_day
                                on latest_day.profile_id = profile_holdings_normalized.profile_id
                                    and latest_day.holding_id_v2 = profile_holdings_normalized.holding_id_v2
                                    and latest_day.symbol = profile_holdings_normalized.symbol
                                    and latest_day.idx = 1
                      left join daily_latest_chart_point previous_day
                                on previous_day.profile_id = profile_holdings_normalized.profile_id
                                    and previous_day.holding_id_v2 = profile_holdings_normalized.holding_id_v2
                                    and previous_day.symbol = profile_holdings_normalized.symbol
                                    and previous_day.idx = 2
             where is_app_trading
     )
select *
from data

{% if is_incremental() %}
         left join {{ this }} old_data using (profile_id, holding_id_v2, symbol, date)
where old_data.profile_id is null
   or abs(data.relative_daily_gain - old_data.relative_daily_gain) > 1e-3
   or abs(data.value - old_data.value) > 1e-3
{% endif %}
