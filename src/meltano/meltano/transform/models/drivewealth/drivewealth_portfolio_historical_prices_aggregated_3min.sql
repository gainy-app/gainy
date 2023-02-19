{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('profile_id, holding_id_v2, symbol, datetime'),
      index('id', true),
      index('updated_at'),
    ]
  )
}}

    
with portfolio_statuses as
         (
             select distinct on (profile_id, datetime) *
             from (
                      select profile_id,
                             (date_trunc('minute', drivewealth_portfolio_statuses.created_at) -
                              interval '1 minute' *
                              mod(extract(minutes from drivewealth_portfolio_statuses.created_at)::int, 3)
                                 )::timestamp                                  as datetime,
                             drivewealth_portfolio_statuses.created_at         as updated_at,
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
{% if var('realtime') %}
                      where drivewealth_portfolio_statuses.created_at > (select max(updated_at) from {{ this }}) - interval '30 minutes'
{% else %}
                      where drivewealth_portfolio_statuses.created_at > now() - interval '5 days'
{% endif %}
                  ) t
             order by profile_id, datetime, updated_at desc
         ),
     portfolio_status_funds as
         (
             select profile_id,
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
                    date,
                    datetime,
                    updated_at,
                    (fund_holding_data ->> 'value')::numeric                        as value
             from fund_holdings

             union all

             select profile_id,
                    profile_id || '_cash_CUR:USD'                 as holding_id_v2,
                    'CUR:USD'                                     as symbol,
                    null                                          as collection_id,
                    date,
                    datetime,
                    updated_at,
                    (portfolio_holding_data ->> 'value')::numeric as value
             from portfolio_status_funds
             where portfolio_holding_data ->> 'type' = 'CASH_RESERVE'

             union all

             select profile_id,
                    holding_id_v2,
                    symbol,
                    collection_id,
                    (date + interval '1 day') as date,
                    (date + interval '1 day')::timestamp as datetime,
                    null as updated_at,
                    t.value
             from {{ ref('drivewealth_portfolio_historical_holdings') }} t
             where date > now() - interval '5 days'
     ),
     profile_date_threshold as
         (
             select profile_id, min(datetime) as datetime_threshold
             from (
                      select profile_id, max(datetime) as datetime
                      from {{ ref('drivewealth_holdings') }}
                               join {{ ref('historical_prices_aggregated_3min') }} using (symbol)
                      group by profile_id, symbol
                  ) t
             group by profile_id
     ),
     schedule as
         (
             with min_holding_date as materialized
                      (
                          select profile_id,
                                 holding_id_v2,
                                 symbol,
                                 collection_id,
                                 min(date)     as min_date,
                                 min(datetime) as min_datetime
                          from data
                          group by profile_id, holding_id_v2, collection_id, symbol
                      ),
                  ticker_schedule as materialized
                      (
                          select profile_id,
                                 holding_id_v2,
                                 collection_id,
                                 symbol,
                                 date,
                                 datetime,
                                 coalesce(relative_gain, 0) as relative_gain
                          from min_holding_date
                                   join profile_date_threshold using (profile_id)
                                   left join {{ ref('historical_prices_aggregated_3min') }} using (symbol)
                          where historical_prices_aggregated_3min.datetime <= datetime_threshold
{% if var('realtime') %}
                            and historical_prices_aggregated_3min.datetime >= min_datetime
{% else %}
                            and historical_prices_aggregated_3min.date >= min_date
{% endif %}
                  )
             select profile_id,
                    holding_id_v2,
                    collection_id,
                    symbol,
                    date,
                    datetime,
                    relative_gain
             from ticker_schedule

             union all

             select profile_id,
                    profile_id || '_cash_CUR:USD' as holding_id_v2,
                    null                          as collection_id,
                    'CUR:USD'                     as symbol,
                    date,
                    datetime,
                    0                             as relative_gain
             from (
                      select profile_id, date, datetime
                      from ticker_schedule
                      group by profile_id, date, datetime
                  ) t
     ),
     data_combined as
         (
             select profile_id,
                    holding_id_v2,
                    symbol,
                    collection_id,
                    date,
                    datetime,
                    updated_at,
                    value,
                    null  as relative_gain,
                    false as is_scheduled
             from data

             union all

             select profile_id,
                    holding_id_v2,
                    schedule.symbol,
                    schedule.collection_id,
                    schedule.date,
                    datetime,
                    coalesce(data.updated_at, schedule.updated_at) as updated_at,
                    data.value,
                    relative_gain,
                    true as is_scheduled
             from schedule
                      left join data using (profile_id, holding_id_v2, datetime)
     ),
     data_combined1 as
         (
             select profile_id,
                    holding_id_v2,
                    symbol,
                    collection_id,
                    date,
                    datetime,
                    value,
                    updated_at,
                    relative_gain,
                    exp(sum(ln(relative_gain + 1 + 1e-10)) over wnd) as cumulative_relative_gain,
                    is_scheduled
             from data_combined
                 window wnd as (partition by profile_id, holding_id_v2 order by datetime)
     ),
    data_combined2 as
         (
             select profile_id,
                    holding_id_v2,
                    symbol,
                    collection_id,
                    t.date,
                    datetime,
                    t.updated_at,
                    relative_gain,
                    case
                        when t.value is not null
                            then t.value
                        -- if value is null but no portfolio_statuses exist in this day - then we assume there is value, just it's record is missing
                        when portfolio_statuses.profile_id is null
                            then cumulative_relative_gain *
                                 (last_value_ignorenulls(t.value / coalesce(cumulative_relative_gain, 1)) over wnd)
                        else 0
                        end as value,
                    is_scheduled
             from data_combined1 t
                      left join portfolio_statuses using (profile_id, datetime)
                 window wnd as (partition by profile_id, holding_id_v2 order by datetime)
     ),
     data_extended as
         (
             select profile_id,
                    holding_id_v2,
                    symbol,
                    collection_id,
                    date,
                    datetime,
                    coalesce(updated_at, now()) as updated_at,
                    relative_gain,
                    value,
                    coalesce(lag(value) over wnd, 0) as prev_value
             from data_combined2
             where is_scheduled
                 window wnd as (partition by profile_id, holding_id_v2 order by datetime)
     )
select data_extended.*,
       profile_id || '_' || holding_id_v2 || '_' || datetime as id
from data_extended

{% if is_incremental() %}
         left join {{ this }} old_data using (profile_id, holding_id_v2, symbol, datetime)
where old_data.profile_id is null
   or abs(data_extended.value - old_data.value) > 1e-3
   or abs(data_extended.prev_value - old_data.prev_value) > 1e-3
   or abs(data_extended.relative_gain - old_data.relative_gain) > 1e-5
{% endif %}
