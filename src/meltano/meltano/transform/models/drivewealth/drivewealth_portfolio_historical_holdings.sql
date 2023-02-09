{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('profile_id, holding_id_v2, symbol, date'),
      index('id', true),
      index('portfolio_status_id', false),
      'create index if not exists "dphh_profile_id_holding_id_v2_symbol_date_week" ON {{ this }} (profile_id, holding_id_v2, symbol, date_week)',
      'create index if not exists "dphh_profile_id_holding_id_v2_symbol_date_month" ON {{ this }} (profile_id, holding_id_v2, symbol, date_month)',
    ]
  )
}}

-- IF CHANGED - CHANGE drivewealth_latest_cashflow.SQL AS WELL

with portfolio_statuses as
         (
             select distinct on (
                 profile_id, date
                 ) profile_id,
                   drivewealth_portfolio_statuses.created_at         as updated_at,
                   drivewealth_portfolio_statuses.id                 as portfolio_status_id,
                   drivewealth_portfolio_statuses.date,
                   case
                       when drivewealth_portfolio_statuses.cash_actual_weight > 0
                           then cash_value / drivewealth_portfolio_statuses.cash_actual_weight
                       else drivewealth_portfolio_statuses.equity_value
                       end                                           as value,
                   drivewealth_portfolio_statuses.data -> 'holdings' as holdings
             from {{ source('app', 'drivewealth_portfolio_statuses') }}
                      join {{ source('app', 'drivewealth_portfolios') }}
                           on drivewealth_portfolios.ref_id = drivewealth_portfolio_id
             order by profile_id desc, date desc, drivewealth_portfolio_statuses.created_at desc
         ),
     portfolio_status_funds as
         (
             select profile_id,
                    portfolio_status_id,
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
                    portfolio_status_id,
                    portfolio_status_funds.updated_at,
                    portfolio_holding_data,
                    json_array_elements(portfolio_holding_data -> 'holdings') as fund_holding_data
             from portfolio_status_funds
                      join {{ source('app', 'drivewealth_funds') }}
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
                    collection_id,
                    portfolio_status_id,
                    date,
                    (fund_holding_data ->> 'value')::numeric                        as value,
                    updated_at
             from fund_holdings
     ),
     schedule as
         (
             with min_holding_date as materialized
                      (
                          select profile_id,
                                 holding_id_v2,
                                 symbol,
                                 min(date) as min_date
                          from data
                          group by profile_id, holding_id_v2, symbol
                      )
             select profile_id, holding_id_v2, symbol, date, relative_daily_gain
             from min_holding_date
                      join {{ ref('historical_prices') }} using (symbol)
             where date >= min_date

             union all

             select profile_id, holding_id_v2, symbol, date, relative_daily_change as relative_daily_gain
             from min_holding_date
                      join {{ ref('ticker_realtime_metrics') }} using (symbol)
                      left join {{ ref('historical_prices') }} using (symbol, date)
             where date >= min_date
               and historical_prices.symbol is null
     ),
     data_extended0 as
         (
             select profile_id,
                    holding_id_v2,
                    LAST_VALUE_IGNORENULLS(portfolio_status_id) over wnd as portfolio_status_id,
                    LAST_VALUE_IGNORENULLS(collection_id) over wnd       as collection_id,
                    symbol,
                    date,
                    relative_daily_gain,
                    value                                                as value,
                    data.updated_at
             from schedule
                      left join data using (profile_id, holding_id_v2, symbol, date)
             window wnd as (partition by profile_id, holding_id_v2 order by date)
     ),
     data_extended1 as
         (
             with historical_holdings_extended as
                      (
                          select profile_id,
                                 holding_id_v2,
                                 date,
                                 collection_id,
                                 symbol,
                                 portfolio_status_id,
                                 relative_daily_gain,
                                 value,
                                 coalesce(lag(value) over wnd, 0) as prev_value,
                                 updated_at
                          from data_extended0
                              window wnd as (partition by holding_id_v2 order by date)
                      )
             select holding_id_v2,
                    profile_id,
                    collection_id,
                    symbol,
                    t.portfolio_status_id,
                    date,
                    t.value,
                    prev_value,
                    cash_flow,
                    t.updated_at,
                    case
                        when t.value > 0 and t.prev_value > 0
                            then relative_daily_gain
                        -- if value is null but no portfolio_statuses exist in this day - then we assume there is value, just it's record is missing
                        when (t.value is null and portfolio_statuses.profile_id is null) and t.prev_value > 0
                            then relative_daily_gain
                        when t.prev_value < 1e-10 and cash_flow > 0
                            then t.value / cash_flow - 1
                        when (t.value < 1e-10 or (t.value is null and portfolio_statuses.profile_id is null)) and prev_value > 0
                            then -1
                        end as relative_daily_gain
             from (
                      select holding_id_v2,
                             profile_id,
                             collection_id,
                             symbol,
                             portfolio_status_id,
                             date,
                             relative_daily_gain,
                             value,
                             prev_value,
                             -- CF = EV / (HP + 1) - BV
                             case
                                 when relative_daily_gain > -1
                                     then coalesce(value / (relative_daily_gain + 1) - prev_value, 0)
                                 else prev_value * relative_daily_gain
                                 end as cash_flow,
                             historical_holdings_extended.updated_at
                      from historical_holdings_extended
                  ) t
                      left join portfolio_statuses using (profile_id, date)
     ),
     data_extended2 as
         (
             select *,
                    exp(sum(ln(relative_daily_gain + 1 + 1e-10)) over wnd) as cumulative_daily_relative_gain
             from data_extended1
                 window wnd as (partition by holding_id_v2 order by date)
     ),
     data_extended3 as
         (
             select profile_id,
                    data.portfolio_status_id,
                    holding_id_v2,
                    collection_id,
                    symbol,
                    date,
                    cash_flow,
                    relative_daily_gain,
                    case
                        when data.value is not null
                            then data.value
                        when portfolio_statuses.profile_id is null
                            then cumulative_daily_relative_gain *
                                 (public.last_value_ignorenulls(data.value / cumulative_daily_relative_gain) over wnd)
                        else 0
                        end                                                 as value,
                    public.last_value_ignorenulls(data.updated_at) over wnd as updated_at
             from data_extended2 data
                      left join portfolio_statuses using (profile_id, date)
                 window wnd as (partition by profile_id, holding_id_v2 order by date rows between unbounded preceding and current row)
     ),
     data_extended as
         (
             select *,
                    coalesce(lag(value) over wnd, 0) as prev_value
             from data_extended3
                 window wnd as (partition by profile_id, holding_id_v2 order by date)
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
