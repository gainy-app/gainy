{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}


(
    with collection_symbol_realtime_gains as
         (
             select profile_id,
                    collection_id,
                    collection_uniq_id,
                    date,
                    week_trading_sessions_static.prev_date,
                    datetime,
                    case
                        when collection_ticker_weights.price > 0
                            then weight * (open / collection_ticker_weights.price - 1)
                        else 0 end as open_gain,
                    case
                        when collection_ticker_weights.price > 0
                            then weight * (high / collection_ticker_weights.price - 1)
                        else 0 end as high_gain,
                    case
                        when collection_ticker_weights.price > 0
                            then weight * (low / collection_ticker_weights.price - 1)
                        else 0 end as low_gain,
                    case
                        when collection_ticker_weights.price > 0
                            then weight * (close / collection_ticker_weights.price - 1)
                        else 0 end as close_gain,
                    case
                        when collection_ticker_weights.price > 0
                            then weight * (adjusted_close / collection_ticker_weights.price - 1)
                        else 0 end as adjusted_close_gain
             from {{ ref('collection_ticker_weights') }}
                      join {{ ref('historical_prices_aggregated_3min') }} using (symbol, date)
                      join {{ ref('week_trading_sessions') }} using (symbol, date)
             where week_trading_sessions.index = 0
               and historical_prices_aggregated_3min.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at
         ),
    collection_realtime_gains as
        (
            select profile_id,
                   collection_id,
                   collection_uniq_id,
                   date,
                   max(prev_date) as prev_date,
                   datetime,
                   sum(open_gain) + 1 as open_gain,
                   sum(high_gain) + 1 as high_gain,
                   sum(low_gain) + 1 as low_gain,
                   sum(close_gain) + 1 as close_gain,
                   sum(adjusted_close_gain) + 1 as adjusted_close_gain
            from collection_symbol_realtime_gains
            group by profile_id, collection_id, collection_uniq_id, date, datetime
     )
    select collection_realtime_gains.profile_id,
           collection_realtime_gains.collection_id,
           collection_realtime_gains.collection_uniq_id,
           collection_realtime_gains.datetime,
           '1d'                                                     as period,
           collection_historical_values.value * open_gain           as open,
           collection_historical_values.value * high_gain           as high,
           collection_historical_values.value * low_gain            as low,
           collection_historical_values.value * close_gain          as close,
           collection_historical_values.value * adjusted_close_gain as adjusted_close
    from collection_realtime_gains
         join {{ ref('collection_historical_values') }}
             on collection_historical_values.collection_uniq_id = collection_realtime_gains.collection_uniq_id
                and collection_historical_values.date = collection_realtime_gains.prev_date
)

union all

(
    with collection_symbol_realtime_gains as
         (
             select profile_id,
                    collection_id,
                    collection_uniq_id,
                    date,
                    week_trading_sessions_static.prev_date,
                    datetime,
                    case
                        when collection_ticker_weights.price > 0
                            then weight * (open / collection_ticker_weights.price - 1)
                        else 0 end as open_gain,
                    case
                        when collection_ticker_weights.price > 0
                            then weight * (high / collection_ticker_weights.price - 1)
                        else 0 end as high_gain,
                    case
                        when collection_ticker_weights.price > 0
                            then weight * (low / collection_ticker_weights.price - 1)
                        else 0 end as low_gain,
                    case
                        when collection_ticker_weights.price > 0
                            then weight * (close / collection_ticker_weights.price - 1)
                        else 0 end as close_gain,
                    case
                        when collection_ticker_weights.price > 0
                            then weight * (adjusted_close / collection_ticker_weights.price - 1)
                        else 0 end as adjusted_close_gain
             from {{ ref('collection_ticker_weights') }}
                      join {{ ref('historical_prices_aggregated_15min') }} using (symbol, date)
                      join {{ ref('week_trading_sessions') }} using (symbol, date)
             where historical_prices_aggregated_15min.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at
         ),
    collection_realtime_gains as
        (
            select profile_id,
                   collection_id,
                   collection_uniq_id,
                   date,
                   max(prev_date) as prev_date,
                   datetime,
                   sum(open_gain) + 1 as open_gain,
                   sum(high_gain) + 1 as high_gain,
                   sum(low_gain) + 1 as low_gain,
                   sum(close_gain) + 1 as close_gain,
                   sum(adjusted_close_gain) + 1 as adjusted_close_gain
            from collection_symbol_realtime_gains
            group by profile_id, collection_id, collection_uniq_id, date, datetime
     )
    select collection_realtime_gains.profile_id,
           collection_realtime_gains.collection_id,
           collection_realtime_gains.collection_uniq_id,
           collection_realtime_gains.datetime,
           '1w'                                                     as period,
           collection_historical_values.value * open_gain           as open,
           collection_historical_values.value * high_gain           as high,
           collection_historical_values.value * low_gain            as low,
           collection_historical_values.value * close_gain          as close,
           collection_historical_values.value * adjusted_close_gain as adjusted_close
    from collection_realtime_gains
         join {{ ref('collection_historical_values') }}
             on collection_historical_values.collection_uniq_id = collection_realtime_gains.collection_uniq_id
                and collection_historical_values.date = collection_realtime_gains.prev_date
)

union all

(
    select profile_id,
           collection_id,
           collection_uniq_id,
           date::timestamp as datetime,
           '1m'            as period,
           value           as open,
           value           as high,
           value           as low,
           value           as close,
           value           as adjusted_close
    from {{ ref('collection_historical_values') }}
    where collection_historical_values.date >= now() - interval '1 month'
)

union all

(
    select profile_id,
           collection_id,
           collection_uniq_id,
           date::timestamp as datetime,
           '3m'            as period,
           value           as open,
           value           as high,
           value           as low,
           value           as close,
           value           as adjusted_close
    from {{ ref('collection_historical_values') }}
    where collection_historical_values.date >= now() - interval '3 month'
)

union all

(
    select profile_id,
           collection_id,
           collection_uniq_id,
           date::timestamp as datetime,
           '1y'            as period,
           value           as open,
           value           as high,
           value           as low,
           value           as close,
           value           as adjusted_close
    from {{ ref('collection_historical_values') }}
    where collection_historical_values.date >= now() - interval '1 year'
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
