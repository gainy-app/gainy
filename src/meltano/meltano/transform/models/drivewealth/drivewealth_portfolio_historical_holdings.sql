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

with portfolio_statuses as
         (
             select distinct on (profile_id, date) *
             from (
                      select profile_id,
                             drivewealth_portfolio_statuses.created_at                              as updated_at,
                             (drivewealth_portfolio_statuses.created_at - interval '5 hours')::date as date,
                             cash_value / drivewealth_portfolio_statuses.cash_actual_weight         as value,
                             drivewealth_portfolio_statuses.data -> 'holdings'                      as holdings
                      from {{ source('app', 'drivewealth_portfolio_statuses')}}
                               join {{ source('app', 'drivewealth_portfolios')}}
                                    on drivewealth_portfolios.ref_id = drivewealth_portfolio_id
                  ) t
             order by profile_id, date desc, updated_at desc
         ),
     portfolio_status_funds as
         (
             select profile_id,
                    date,
                    value,
                    updated_at,
                    json_array_elements(holdings) as portfolio_holding_data
             from portfolio_statuses
     ),
     fund_holdings as
         (
             select portfolio_status_funds.profile_id,
                    portfolio_status_funds.date,
                    drivewealth_funds.collection_id,
                    portfolio_status_funds.updated_at,
                    portfolio_holding_data,
                    json_array_elements(portfolio_holding_data -> 'holdings') as fund_holding_data
             from portfolio_status_funds
                      join {{ source('app', 'drivewealth_funds')}}
                           on drivewealth_funds.ref_id = portfolio_holding_data ->> 'id'
             where portfolio_holding_data ->> 'type' != 'CASH_RESERVE'
     ),
     data as
         (
             select profile_id,
                    case
                        when collection_id is null
                            then 'dw_ticker_' || profile_id || '_' || normalize_drivewealth_symbol(fund_holding_data ->> 'symbol')
                        else 'dw_ttf_' || profile_id || '_' || collection_id || '_' || normalize_drivewealth_symbol(fund_holding_data ->> 'symbol')
                        end                                                         as holding_id_v2,
                    normalize_drivewealth_symbol(fund_holding_data ->> 'symbol') as symbol,
                    date,
                    (fund_holding_data ->> 'value')::numeric                        as value,
                    updated_at
             from fund_holdings
     ),
     schedule as
         (
             select profile_id, holding_id_v2, symbol, dd::date as date
             from (
                      select profile_id,
                             holding_id_v2,
                             symbol,
                             min(date) as min_date
                      from data
                      group by profile_id, holding_id_v2, symbol
                  ) t
                      join {{ ref('ticker_realtime_metrics') }} using (symbol)
                      join generate_series(min_date, ticker_realtime_metrics.time::date, interval '1 day') dd on true
     ),
     data_extended0 as
         (
             select profile_id,
                    holding_id_v2,
                    symbol,
                    date,
                    value                                                as value,
                    data.updated_at
             from schedule
                      left join data using (profile_id, holding_id_v2, symbol, date)
                      left join {{ ref('historical_prices') }} using (symbol, date)
                      left join {{ ref('ticker_realtime_metrics') }} using (symbol, date)
     ),
     data_extended1 as
         (
             with historical_holdings_extended as
                      (
                          select profile_id,
                                 holding_id_v2,
                                 date,
                                 symbol,
                                 value,
                                 coalesce(lag(value) over wnd, 0) as prev_value,
                                 updated_at
                          from data_extended0
                              window wnd as (partition by holding_id_v2 order by date)
                      ),
                  filled_orders as
                      (
                          -- todo refactor orders
                          select profile_id,
                                 symbol,
                                 case when drivewealth_orders.data ->> 'side' = 'SELL' then -1 else 1 end *
                                 abs((drivewealth_orders.data ->> 'totalOrderAmount')::numeric) as amount,
                                 (drivewealth_orders.data ->> 'lastExecuted')::timestamptz      as last_executed_at,
                                 ((drivewealth_orders.data ->> 'lastExecuted')::timestamptz AT TIME ZONE
                                  'America/New_York')::date                                     as date,
                                 drivewealth_orders.ref_id
                          from {{ source('app', 'drivewealth_orders') }}
                                   join {{ source('app', 'drivewealth_accounts') }} on drivewealth_accounts.ref_id = account_id
                                   join {{ source('app', 'drivewealth_users') }} on drivewealth_users.ref_id = drivewealth_accounts.drivewealth_user_id
                          where drivewealth_orders.status = 'FILLED'
                  ),
                  ticker_values_aggregated as
                      (
                          select profile_id, symbol, date, sum(value) as value, sum(prev_value) as prev_value
                          from historical_holdings_extended
                          where profile_id = 67
                          group by profile_id, symbol, date
                  ),
                  order_values_aggregated as
                      (
                          select profile_id, symbol, date, sum(amount) as amount
                          from filled_orders
                          where profile_id = 67
                          group by profile_id, symbol, date
                  ),
                  ticker_stats as
                      (
                          select profile_id,
                                 symbol,
                                 date,
                                 -- HP = EV / (BV + CF) - 1
                                 ticker_values_aggregated.value /
                                 (ticker_values_aggregated.prev_value + order_values_aggregated.amount) - 1 as gain
                          from ticker_values_aggregated
                                   left join order_values_aggregated using (profile_id, symbol, date)
                  )
             select *,
                    case
                        when prev_value + cash_flow > 0
                            then value / (prev_value + cash_flow) - 1
                        else 0
                        end as relative_daily_gain
             from (
                      select holding_id_v2,
                             profile_id,
                             symbol,
                             date,
                             value,
                             prev_value,
                             -- CF = EV / (HP + 1) - BV
                             coalesce(value / (gain + 1) - prev_value, 0) as cash_flow,
                             updated_at
                      from historical_holdings_extended
                               left join ticker_stats using (profile_id, symbol, date)
                  ) t
     ),
     data_extended2 as
         (
             select *,
                    exp(sum(ln(relative_daily_gain + 1)) over wnd) as cumulative_daily_relative_gain
             from data_extended1
                 window wnd as (partition by holding_id_v2 order by date)
     ),
     data_extended as
         (
             select profile_id,
                    holding_id_v2,
                    symbol,
                    date,
                    cash_flow,
                    relative_daily_gain,
                    cumulative_daily_relative_gain *
                    (public.last_value_ignorenulls(value / cumulative_daily_relative_gain) over wnd) as value,
                    public.last_value_ignorenulls(updated_at) over wnd                as updated_at
             from data_extended2
                 window wnd as (partition by profile_id, holding_id_v2 order by date rows between unbounded preceding and current row)
     )
select data_extended.*,
       date_trunc('week', date)::date                    as date_week,
       date_trunc('month', date)::date                   as date_month,
       profile_id || '_' || holding_id_v2 || '_' || date as id
from data_extended

{% if is_incremental() %}
         left join {{ this }} old_data using (profile_id, holding_id_v2, symbol, date)
where old_data.profile_id is null
   or abs(data_extended.relative_daily_gain - old_data.relative_daily_gain) > 1e-3
   or abs(data_extended.value - old_data.value) > 1e-3
{% endif %}
