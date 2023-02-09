{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('profile_id, holding_id_v2, symbol, datetime'),
      index('id', true),
      index('portfolio_status_id'),
    ]
  )
}}

    
with portfolio_statuses as
         (
             select profile_id,
                    (date_trunc('minute', drivewealth_portfolio_statuses.created_at) -
                     interval '1 minute' * mod(extract(minutes from drivewealth_portfolio_statuses.created_at)::int, 15)
                        )::timestamp                                  as datetime,
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
             where drivewealth_portfolio_statuses.created_at > now() - interval '10 days'
{% if var('realtime') %}
               and drivewealth_portfolio_statuses.id > (select max(portfolio_status_id) from {{ this }})
{% endif %}
         ),
     portfolio_status_funds as
         (
             select profile_id,
                    portfolio_status_id,
                    date,
                    datetime,
                    value,
                    updated_at,
                    json_array_elements(holdings) as portfolio_holding_data
             from portfolio_statuses
     ),
     fund_holdings as
         (
             select portfolio_status_funds.profile_id,
                    portfolio_status_funds.date,
                    portfolio_status_funds.datetime,
                    portfolio_status_funds.updated_at,
                    drivewealth_funds.collection_id,
                    portfolio_status_id,
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
                            then 'dw_ticker_' || profile_id || '_' ||
                                 normalize_drivewealth_symbol(fund_holding_data ->> 'symbol')
                        else 'dw_ttf_' || profile_id || '_' || collection_id || '_' ||
                             normalize_drivewealth_symbol(fund_holding_data ->> 'symbol')
                        end                                                         as holding_id_v2,
                    normalize_drivewealth_symbol(fund_holding_data ->> 'symbol') as symbol,
                    collection_id,
                    portfolio_status_id,
                    date,
                    datetime,
                    updated_at,
                    (fund_holding_data ->> 'value')::numeric                        as value
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
             select profile_id, holding_id_v2, symbol, date, datetime, relative_gain, updated_at
             from min_holding_date
                      join {{ ref('historical_prices_aggregated_15min') }} using (symbol)
             where historical_prices_aggregated_15min.date >= min_date
     ),
     data_extended0 as
         (
             select profile_id,
                    holding_id_v2,
                    LAST_VALUE_IGNORENULLS(portfolio_status_id) over wnd as portfolio_status_id,
                    LAST_VALUE_IGNORENULLS(collection_id) over wnd       as collection_id,
                    symbol,
                    schedule.date,
                    datetime,
                    relative_gain,
                    value                                                as value,
                    greatest(schedule.updated_at, data.updated_at)       as updated_at
             from schedule
                      left join data using (profile_id, holding_id_v2, symbol, datetime)
                 window wnd as (partition by profile_id, holding_id_v2 order by datetime)
     ),
     data_extended2 as
         (
             select *,
                    exp(sum(ln(relative_gain + 1 + 1e-10)) over wnd) as cumulative_daily_relative_gain
             from data_extended0
                 window wnd as (partition by holding_id_v2 order by datetime)
     ),
     data_extended3 as
         (
             select profile_id,
                    portfolio_status_id,
                    holding_id_v2,
                    collection_id,
                    symbol,
                    date,
                    datetime,
                    relative_gain,
                    cumulative_daily_relative_gain,
                    updated_at,
                    case
                        when value is not null
                            then value
                        when exists(select profile_id
                                    from portfolio_statuses
                                    where portfolio_statuses.profile_id = data.profile_id
                                      and portfolio_statuses.datetime = data.datetime)
                            then 0
                        end as value
             from data_extended2 data
     ),
     data_extended4 as
         (
             select profile_id,
                    portfolio_status_id,
                    holding_id_v2,
                    collection_id,
                    symbol,
                    date,
                    datetime,
                    relative_gain,
                    updated_at,
                    coalesce(value, cumulative_daily_relative_gain *
                                    (public.last_value_ignorenulls(value / cumulative_daily_relative_gain)
                                     over wnd)
                        ) as value
             from data_extended3
                 window wnd as (partition by profile_id, holding_id_v2 order by datetime)
     ),
     data_extended as
         (
             select *,
                    coalesce(lag(value) over wnd, 0) as prev_value
             from data_extended4
                 window wnd as (partition by profile_id, holding_id_v2 order by datetime)
     )
select data_extended.*,
       profile_id || '_' || holding_id_v2 || '_' || datetime as id
from data_extended

{% if is_incremental() %}
         left join {{ this }} old_data using (profile_id, holding_id_v2, symbol, datetime)
where old_data.profile_id is null
   or data_extended.updated_at > old_data.updated_at
{% endif %}
