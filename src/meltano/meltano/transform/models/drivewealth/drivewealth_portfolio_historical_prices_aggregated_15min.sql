{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('holding_id_v2, datetime'),
      index('id', true),
      index('updated_at'),
      'delete from {{ this }} where datetime < now() - interval \'2 weeks\'',
    ]
  )
}}


with data as
         (
             select distinct on (
                 profile_id, holding_id_v2, symbol, date, datetime_15min
                 ) profile_id,
                   holding_id_v2,
                   portfolio_status_id,
                   symbol,
                   collection_id,
                   date,
                   datetime_15min as datetime,
                   updated_at,
                   value
             from {{ ref('drivewealth_portfolio_historical_holdings_intraday') }}
{% if var('realtime') %}
                       left join (
                                     select profile_id, max(date) as max_date
                                     from {{ source('app', 'drivewealth_portfolio_statuses') }}
                                              join {{ source('app', 'drivewealth_portfolios') }}
                                                   on drivewealth_portfolios.ref_id = drivewealth_portfolio_id
                                     group by profile_id
                                 ) last_portfolio_update using (profile_id)
             where date >= coalesce(greatest(last_portfolio_update.max_date, now() - interval '5 days'), now() - interval '5 days')
{% else %}
             where date > now() - interval '10 days'
{% endif %}
             order by profile_id, holding_id_v2, symbol, date, datetime_15min, datetime_3min desc
     ),
     profile_date_threshold as
         (
             select profile_id, min(datetime) as datetime_threshold
             from (
                      select profile_id, max(datetime) as datetime
                      from {{ ref('drivewealth_holdings') }}
                               join {{ ref('historical_prices_aggregated_15min') }} using (symbol)
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
                      )
             select profile_id,
                    holding_id_v2,
                    collection_id,
                    symbol,
                    date,
                    datetime,
                    coalesce(relative_gain, 0) as relative_gain
             from min_holding_date
                      join profile_date_threshold using (profile_id)
                      left join {{ ref('historical_prices_aggregated_15min') }} using (symbol)
             where historical_prices_aggregated_15min.datetime <= datetime_threshold
{% if var('realtime') %}
               and historical_prices_aggregated_15min.datetime >= min_datetime
               and min_datetime <= datetime_threshold
{% else %}
               and historical_prices_aggregated_15min.date >= min_date
{% endif %}

             union all

             select profile_id,
                    holding_id_v2,
                    collection_id,
                    min_holding_date.symbol,
                    date,
                    datetime,
                    coalesce(relative_gain, 0) as relative_gain
             from min_holding_date
                      left join profile_date_threshold using (profile_id)
                      left join {{ ref('historical_prices_aggregated_15min') }} on historical_prices_aggregated_15min.symbol = 'SPY'
             where min_holding_date.symbol = 'CUR:USD'
               and (historical_prices_aggregated_15min.datetime <= datetime_threshold or datetime_threshold is null)
{% if var('realtime') %}
               and historical_prices_aggregated_15min.datetime >= min_datetime
               and (min_datetime <= datetime_threshold or datetime_threshold is null)
{% else %}
               and historical_prices_aggregated_15min.date >= min_date
{% endif %}
     ),
     data_combined as materialized
         (
             select profile_id,
                    holding_id_v2,
                    portfolio_status_id,
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
                    last_value_ignorenulls(portfolio_status_id) over wnd as portfolio_status_id,
                    schedule.symbol,
                    schedule.collection_id,
                    schedule.date,
                    datetime,
                    data.updated_at,
                    data.value,
                    relative_gain,
                    true as is_scheduled
             from schedule
                      left join data using (profile_id, holding_id_v2, datetime)
                 window wnd as (partition by profile_id, holding_id_v2 order by datetime)
     ),
     data_combined1 as
         (
             select profile_id,
                    holding_id_v2,
                    portfolio_status_id,
                    symbol,
                    collection_id,
                    date,
                    datetime,
                    value,
                    updated_at,
                    relative_gain,
                    exp(sum(ln(relative_gain + 1 + {{ var('epsilon') }})) over wnd) as cumulative_relative_gain,
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
                        when (portfolio_status_id = latest_portfolio_status_id) or (portfolio_status_id is null and latest_portfolio_status_id is null)
                            then cumulative_relative_gain *
                                 (last_value_ignorenulls(t.value / coalesce(cumulative_relative_gain, 1)) over wnd)
                        else 0
                        end as value,
                    is_scheduled
             from data_combined1 t
                      left join (
                                    select profile_id, datetime, max(portfolio_status_id) as latest_portfolio_status_id
                                    from data_combined
                                    group by profile_id, datetime
                                ) stats using (profile_id, datetime)
             where portfolio_status_id = latest_portfolio_status_id or is_scheduled
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
                    LAST_VALUE_IGNORENULLS(updated_at) over wnd as updated_at,
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
{% endif %}

where data_extended.value is not null
  
{% if is_incremental() %}
  and (old_data.profile_id is null
    or abs(data_extended.value - old_data.value) > {{ var('price_precision') }}
    or abs(data_extended.prev_value - old_data.prev_value) > {{ var('price_precision') }}
    or abs(data_extended.relative_gain - old_data.relative_gain) > {{ var('gain_precision') }})
{% endif %}
