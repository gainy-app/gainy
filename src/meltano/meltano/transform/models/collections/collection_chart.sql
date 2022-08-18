{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}


(
    select profile_id,
           collection_id,
           collection_uniq_id,
           datetime,
           '1d'                         as period,
           sum(open * weight)           as open,
           sum(high * weight)           as high,
           sum(low * weight)            as low,
           sum(close * weight)          as close,
           sum(adjusted_close * weight) as adjusted_close
    from {{ ref('historical_prices_aggregated_3min') }}
             join {{ ref('collection_ticker_actual_weights') }} using (symbol)
             left join {{ ref('week_trading_sessions') }}
                       on week_trading_sessions.symbol = historical_prices_aggregated_3min.symbol
    where (historical_prices_aggregated_3min.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at
       or (week_trading_sessions is null and historical_prices_aggregated_3min.datetime > now() - interval '1 day'))
      and (week_trading_sessions is null or (week_trading_sessions.date = historical_prices_aggregated_3min.datetime::date and week_trading_sessions.index = 0))
    group by profile_id, collection_id, collection_uniq_id, datetime, period
)

union all

(
    select profile_id,
           collection_id,
           collection_uniq_id,
           historical_prices_aggregated_15min.datetime,
           '1w'                                                            as period,
           sum(historical_prices_aggregated_15min.open * weight)           as open,
           sum(historical_prices_aggregated_15min.high * weight)           as high,
           sum(historical_prices_aggregated_15min.low * weight)            as low,
           sum(historical_prices_aggregated_15min.close * weight)          as close,
           sum(historical_prices_aggregated_15min.adjusted_close * weight) as adjusted_close
    from {{ ref('collection_ticker_weights') }}
             join {{ ref('historical_prices_aggregated_15min') }}
                  on collection_ticker_weights.symbol = historical_prices_aggregated_15min.symbol
                      and collection_ticker_weights.date = historical_prices_aggregated_15min.datetime::date
             left join {{ ref('week_trading_sessions') }}
                  on week_trading_sessions.symbol = historical_prices_aggregated_15min.symbol
    where (historical_prices_aggregated_15min.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at
       or (week_trading_sessions is null and historical_prices_aggregated_15min.datetime > now() - interval '7 days'))
      and (week_trading_sessions is null or week_trading_sessions.date = historical_prices_aggregated_15min.datetime::date)
    group by profile_id, collection_id, collection_uniq_id, datetime, period
)

union all

(
    select profile_id,
           collection_id,
           collection_uniq_id,
           date  as datetime,
           '1m'  as period,
           value as open,
           value as high,
           value as low,
           value as close,
           value as adjusted_close
    from {{ ref('collection_historical_values') }}
    where collection_historical_values.date >= now() - interval '1 month'
    group by profile_id, collection_id, collection_uniq_id, datetime, period
)

union all

(
    select profile_id,
           collection_id,
           collection_uniq_id,
           date  as datetime,
           '3m'  as period,
           value as open,
           value as high,
           value as low,
           value as close,
           value as adjusted_close
    from {{ ref('collection_historical_values') }}
    where collection_historical_values.date >= now() - interval '3 month'
    group by profile_id, collection_id, collection_uniq_id, datetime, period
)

union all

(
    select profile_id,
           collection_id,
           collection_uniq_id,
           date  as datetime,
           '1y'  as period,
           value as open,
           value as high,
           value as low,
           value as close,
           value as adjusted_close
    from {{ ref('collection_historical_values') }}
    where collection_historical_values.date >= now() - interval '1 year'
    group by profile_id, collection_id, collection_uniq_id, datetime, period
)

union all

(
    select profile_id,
           collection_id,
           collection_uniq_id,
           datetime,
           '5y'           as period,
           open,
           high,
           low,
           adjusted_close as close,
           adjusted_close
    from (
              select DISTINCT ON
                    (
                      date_week,
                      collection_uniq_id
                    ) profile_id,
                      collection_id,
                      collection_uniq_id,
                      date_week                                                                                                        as datetime,
                      first_value(value)
                      OVER (partition by date_week, collection_uniq_id order by date rows between current row and unbounded following) as open,
                      max(value)
                      OVER (partition by date_week, collection_uniq_id order by date rows between current row and unbounded following) as high,
                      min(value)
                      OVER (partition by date_week, collection_uniq_id order by date rows between current row and unbounded following) as low,
                      last_value(value)
                      OVER (partition by date_week, collection_uniq_id order by date rows between current row and unbounded following) as adjusted_close
              from {{ ref('collection_historical_values') }}
              where date_week >= now() - interval '5 year' - interval '1 week'
              order by date_week, collection_uniq_id, date
         ) t
    where t.datetime >= now() - interval '5 year'
)

union all

(
    with data as materialized
         (
             select profile_id,
                    collection_id,
                    collection_uniq_id,
                    date_month,
                    mode() within group ( order by date )      as open_date,
                    mode() within group ( order by date desc ) as close_date,
                    max(value)                                 as high,
                    min(value)                                 as low,
                    sum(value)                                 as volume
             from {{ ref('collection_historical_values') }}
             group by profile_id, collection_id, collection_uniq_id, date_month
         )
    select data.profile_id,
           data.collection_id,
           data.collection_uniq_id,
           data.date_month as datetime,
           'all'           as period,
           chv_open.value  as open,
           data.high,
           data.low,
           chv_close.value as close,
           chv_close.value as adjusted_close
    from data
         join {{ ref('collection_historical_values') }} chv_open on chv_open.collection_uniq_id = data.collection_uniq_id and chv_open.date = data.open_date
         join {{ ref('collection_historical_values') }} chv_close on chv_close.collection_uniq_id = data.collection_uniq_id and chv_close.date = data.close_date
)
