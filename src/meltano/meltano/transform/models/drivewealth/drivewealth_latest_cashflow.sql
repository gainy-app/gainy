{{
  config(
    materialized = "view",
  )
}}

-- this model calculates cash flow for the drivewealth_portfolio_statuses that were created after the latest realtime dbt run
-- IF CHANGED - CHANGE drivewealth_portfolio_historical_holdings.SQL AS WELL

with portfolio_statuses as
         (
             select distinct on (profile_id, date) *
             from (
                      select profile_id,
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
                      where drivewealth_portfolio_statuses.id > (select max(portfolio_status_id) from {{ ref('drivewealth_portfolio_historical_holdings') }})
                  ) t
             order by profile_id, date desc, updated_at desc
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
     historical_holdings_extended as
         (
             select data.profile_id,
                    data.holding_id_v2,
                    data.date,
                    data.collection_id,
                    data.symbol,
                    data.portfolio_status_id,
                    relative_daily_change as relative_daily_gain,
                    data.value,
                    t.value as prev_value,
                    data.updated_at
             from data
                      join {{ ref('ticker_realtime_metrics') }} using (symbol, date)
                      join (
                               select distinct on (
                                   profile_id, holding_id_v2, symbol
                                   ) profile_id,
                                     holding_id_v2,
                                     symbol,
                                     value
                               from {{ ref('drivewealth_portfolio_historical_holdings') }}
                               order by profile_id desc, holding_id_v2 desc, symbol desc, date desc
                           ) t using (holding_id_v2)
         ),
     filled_orders as
         (
             select profile_id,
                    symbol,
                    total_order_amount_normalized as amount,
                    drivewealth_orders.date
             from {{ source('app', 'drivewealth_orders') }}
                      join {{ source('app', 'drivewealth_accounts') }} on drivewealth_accounts.ref_id = drivewealth_orders.account_id
                      join {{ source('app', 'drivewealth_users') }} on drivewealth_users.ref_id = drivewealth_accounts.drivewealth_user_id
             where drivewealth_orders.status = 'FILLED'
               and drivewealth_orders.updated_at > (select max(last_order_updated_at) from drivewealth_portfolio_historical_holdings)
     ),
     ticker_values_aggregated as
         (
             select profile_id, symbol, date, sum(value) as value, sum(prev_value) as prev_value
             from historical_holdings_extended
             group by profile_id, symbol, date
     ),
     order_values_aggregated as
         (
             select profile_id, symbol, date, sum(amount) as amount
             from filled_orders
             group by profile_id, symbol, date
     ),
     ticker_stats as
         (
             select profile_id,
                    symbol,
                    date,
                    -- HP = EV / (BV + CF) - 1
                    case
                        when ticker_values_aggregated.prev_value + order_values_aggregated.amount > 0
                            then ticker_values_aggregated.value / (ticker_values_aggregated.prev_value + order_values_aggregated.amount) - 1
                        end as gain
             from ticker_values_aggregated
                      left join order_values_aggregated using (profile_id, symbol, date)
     )
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
           when value is null
               then relative_daily_gain
           when prev_value + cash_flow > 0
               then value / (prev_value + cash_flow) - 1
           else 0
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
                    when gain > -1
                        then coalesce(value / (gain + 1) - prev_value, 0)
                    else 0
                    end as cash_flow,
                updated_at
         from historical_holdings_extended
                  left join ticker_stats using (profile_id, symbol, date)
     ) t
