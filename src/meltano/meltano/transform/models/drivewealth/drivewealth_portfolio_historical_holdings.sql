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
     order_stats as materialized
         (
             select profile_id,
                    symbol_normalized as symbol,
                    date,
                    sum(total_order_amount_normalized)   as order_cf_sum
             from {{ source('app', 'drivewealth_orders') }}
                      join {{ source('app', 'drivewealth_accounts') }}
                           on drivewealth_accounts.ref_id = drivewealth_orders.account_id
                      join {{ source('app', 'drivewealth_users') }}
                           on drivewealth_users.ref_id = drivewealth_accounts.drivewealth_user_id
                      join (select profile_id, max(updated_at) as created_at from portfolio_statuses group by profile_id) last_portfolio_status
                           using (profile_id)
             where drivewealth_orders.last_executed_at < last_portfolio_status.created_at
             group by profile_id, symbol_normalized, date
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

             union all

             select profile_id,
                    profile_id || '_cash_CUR:USD'                 as holding_id_v2,
                    'CUR:USD'                                     as symbol,
                    null                                          as collection_id,
                    portfolio_status_id,
                    date,
                    (portfolio_holding_data ->> 'value')::numeric as value,
                    updated_at
             from portfolio_status_funds
             where portfolio_holding_data ->> 'type' = 'CASH_RESERVE'
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
                      ),
                  ticker_schedule as materialized
                      (
                          select profile_id, holding_id_v2, symbol, date, relative_daily_gain
                          from min_holding_date
                                   join {{ ref('historical_prices') }} using (symbol)
                          where date >= min_date
                      ),
                  cash_schedule as materialized
                      (
                          select profile_id, date
                          from ticker_schedule
                          group by profile_id, date
                      )
             select profile_id, holding_id_v2, symbol, date, relative_daily_gain
             from ticker_schedule

             union all

             select profile_id, profile_id || '_cash_CUR:USD' as holding_id_v2, 'CUR:USD' as symbol, date, 0 as relative_daily_gain
             from cash_schedule

             union all

             select profile_id, profile_id || '_cash_CUR:USD' as holding_id_v2, 'CUR:USD' as symbol, date, 0 as relative_daily_gain
             from (select profile_id, 'SPY' as symbol from ticker_schedule group by profile_id) t
                      join {{ ref('ticker_realtime_metrics') }} using (symbol)
                      left join {{ ref('historical_prices') }} using (symbol, date)
             where historical_prices.symbol is null

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
     data_extended1 as -- calculate cumulative_daily_relative_gain
         (
             select *,
                    exp(sum(ln(relative_daily_gain + 1 + 1e-10)) over wnd) as cumulative_daily_relative_gain
             from data_extended0
                 window wnd as (partition by holding_id_v2 order by date)
     ),
     data_extended2 as -- fill missing values
         (
             select data.profile_id,
                    data.holding_id_v2,
                    data.portfolio_status_id,
                    data.collection_id,
                    data.symbol,
                    data.date,
                    data.relative_daily_gain,
                    case
                        when data.value is not null
                            then data.value
                        -- if value is null but no portfolio_statuses exist in this day - then we assume there is value, just it's record is missing
                        when portfolio_statuses.profile_id is null
                            then cumulative_daily_relative_gain *
                                 (last_value_ignorenulls(data.value / cumulative_daily_relative_gain) over wnd)
                        end as value,
                    data.updated_at
             from data_extended1 data
                      left join portfolio_statuses using (profile_id, date)
                 window wnd as (partition by holding_id_v2 order by date)
     ),
     data_extended3 as -- recalculate relative_daily_gain
         (
             select profile_id,
                    holding_id_v2,
                    portfolio_status_id,
                    collection_id,
                    symbol,
                    date,
                    coalesce(lag(value) over wnd, 0) as prev_value,
                    value,
                    case
                        when value > 0 or coalesce(lag(value) over wnd, 0) > 0
                            then relative_daily_gain
                        else 0
                        end                          as relative_daily_gain,
                    updated_at
             from data_extended2
                 window wnd as (partition by holding_id_v2 order by date)
     ),
     cash_flow_first_guess as materialized
         (
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
                        when relative_daily_gain is not null and relative_daily_gain > -1
                            then coalesce(value / (relative_daily_gain + 1) - prev_value, 0)
                        else prev_value * relative_daily_gain
                        end as cash_flow,
                    updated_at
             from data_extended3
     ),
     cash_flow_first_guess_filled as
         (
             select *,
                    coalesce(lag(value) over wnd, 0) as prev_value
             from (
                      select holding_id_v2,
                             profile_id,
                             collection_id,
                             symbol,
                             portfolio_status_id,
                             date,
                             relative_daily_gain,
                             case
                                 when value is null and (prev_value_sum is null or prev_value_sum < 1e-10)
                                    then 0
                                 when value is null
                                     -- EV = (CF + BV) * (HP + 1)
                                     then greatest(0, ((order_cf_sum - equity_cf_sum) * prev_value / prev_value_sum + prev_value) * (1 + relative_daily_gain))
                                 else value
                                 end as value,
                             case
                                 when value is null and (prev_value_sum is null or prev_value_sum < 1e-10)
                                    then 0
                                 when value is null
                                     then (order_cf_sum - equity_cf_sum) * prev_value / prev_value_sum
                                 else cash_flow
                                 end as cash_flow,
                             updated_at
                      from cash_flow_first_guess
                               left join order_stats using (profile_id, symbol, date)
                               left join (
                                             select profile_id,
                                                    symbol,
                                                    date,
                                                    sum(cash_flow)  as equity_cf_sum
                                             from cash_flow_first_guess
                                             where symbol != 'CUR:USD'
                                             group by profile_id, symbol, date
                                         ) equity_cf_sum using (profile_id, symbol, date)
                               left join (
                                             select profile_id,
                                                    symbol,
                                                    date,
                                                    sum(prev_value) as prev_value_sum
                                             from cash_flow_first_guess
                                             where symbol != 'CUR:USD' and value is null
                                             group by profile_id, symbol, date
                                         ) prev_value_sum using (profile_id, symbol, date)
                  ) t
                 window wnd as (partition by holding_id_v2 order by date)
     ),
     cash_flow as
         (
             select holding_id_v2,
                    profile_id,
                    collection_id,
                    symbol,
                    portfolio_status_id,
                    date,
                    relative_daily_gain,
                    coalesce(value, 0) as value,
                    prev_value,
                    case
                        when symbol = 'CUR:USD'
                            then value - prev_value
                        when abs(equity_cf_sum) > 0 and order_cf_sum is not null
                            then cash_flow / equity_cf_sum * order_cf_sum
                        when order_cf_sum is not null and holdings_cnt > 0
                            then order_cf_sum / holdings_cnt
                        else 0
                        end            as cash_flow,
                    updated_at
             from cash_flow_first_guess_filled
                      left join order_stats using (profile_id, symbol, date)
                      left join (
                                    select profile_id,
                                           symbol,
                                           date,
                                           count(distinct holding_id_v2) as holdings_cnt,
                                           sum(cash_flow) as equity_cf_sum
                                    from cash_flow_first_guess_filled
                                    where symbol != 'CUR:USD'
                                    group by profile_id, symbol, date
                                ) equity_cf_sum using (profile_id, symbol, date)
         ),
     data_extended as
         (
             select holding_id_v2,
                    profile_id,
                    collection_id,
                    symbol,
                    portfolio_status_id,
                    date,
                    value,
                    prev_value,
                    cash_flow,
                    updated_at,
                    case
                        -- whole day
                        when value > 0 and prev_value > 0
                            then relative_daily_gain
                        -- Post CF
                        when value > 0 and cash_flow > 0 -- and t.prev_value = 0
                            then value / cash_flow - 1
                        -- Pre CF
                        when prev_value > 0 -- and t.value = 0
                            then -cash_flow / prev_value - 1
                        else 0
                        end as relative_daily_gain
             from cash_flow
     ),
     profile_date_threshold as
         (
             select profile_id, min(date) as max_date
             from (
                      select profile_id, max(date) as date
                      from data_extended
                      group by profile_id, holding_id_v2
                  ) t
             group by profile_id
     )
select data_extended.*,
       date_trunc('week', date)::date                    as date_week,
       date_trunc('month', date)::date                   as date_month,
       profile_id || '_' || holding_id_v2 || '_' || date as id
from data_extended
         left join profile_date_threshold using (profile_id)

{% if is_incremental() %}
         left join {{ this }} old_data using (profile_id, holding_id_v2, symbol, date)
{% endif %}

where data_extended.date <= profile_date_threshold.max_date

{% if is_incremental() %}
  and (old_data.profile_id is null
   or abs(data_extended.relative_daily_gain - old_data.relative_daily_gain) > 1e-3
   or abs(data_extended.value - old_data.value) > 1e-3)
{% endif %}
