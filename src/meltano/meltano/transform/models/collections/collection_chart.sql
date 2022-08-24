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
           '1d'::varchar as period,
           sum(open * weight)           as open,
           sum(high * weight)           as high,
           sum(low * weight)            as low,
           sum(close * weight)          as close,
           sum(adjusted_close * weight) as adjusted_close
    from {{ ref('historical_prices_aggregated_3min') }}
             join {{ ref('collection_tickers_weighted') }} using (symbol)
             join {{ ref('week_trading_sessions') }} using (symbol)
    where week_trading_sessions.index = 0
      and historical_prices_aggregated_3min.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at
    group by profile_id, collection_id, collection_uniq_id, datetime, period
)

union all

(
    select profile_id,
           collection_id,
           collection_uniq_id,
           datetime,
           '1w'::varchar as period,
           sum(open * weight)           as open,
           sum(high * weight)           as high,
           sum(low * weight)            as low,
           sum(close * weight)          as close,
           sum(adjusted_close * weight) as adjusted_close
    from {{ ref('historical_prices_aggregated_15min') }}
             join {{ ref('collection_tickers_weighted') }} using (symbol)
             join {{ ref('week_trading_sessions') }} using (symbol)
    where historical_prices_aggregated_15min.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at
    group by profile_id, collection_id, collection_uniq_id, datetime, period
)

union all

(
    with latest_open_trading_session as
             (
                 select symbol, max(date) as date
                 from {{ ref('week_trading_sessions') }}
                 where index = 0
                 group by symbol
             )
    select profile_id,
           collection_id,
           collection_uniq_id,
           datetime,
           '1m'::varchar as period,
           sum(open * weight)           as open,
           sum(high * weight)           as high,
           sum(low * weight)            as low,
           sum(close * weight)          as close,
           sum(adjusted_close * weight) as adjusted_close
    from {{ ref('historical_prices_aggregated_1d') }}
             join {{ ref('collection_tickers_weighted') }} using (symbol)
             left join latest_open_trading_session using (symbol)
    where historical_prices_aggregated_1d.datetime >= coalesce(latest_open_trading_session.date, now()) - interval '1 month'
    group by profile_id, collection_id, collection_uniq_id, datetime, period
)

union all

(
    with latest_open_trading_session as
             (
                 select symbol, max(date) as date
                 from {{ ref('week_trading_sessions') }}
                 where index = 0
                 group by symbol
             )
    select profile_id,
           collection_id,
           collection_uniq_id,
           datetime,
           '3m'::varchar as period,
           sum(open * weight)           as open,
           sum(high * weight)           as high,
           sum(low * weight)            as low,
           sum(close * weight)          as close,
           sum(adjusted_close * weight) as adjusted_close
    from {{ ref('historical_prices_aggregated_1d') }}
             join {{ ref('collection_tickers_weighted') }} using (symbol)
             left join latest_open_trading_session using (symbol)
    where historical_prices_aggregated_1d.datetime >= coalesce(latest_open_trading_session.date, now()) - interval '3 month'
    group by profile_id, collection_id, collection_uniq_id, datetime, period
)

union all

(
    with latest_open_trading_session as
             (
                 select symbol, max(date) as date
                 from {{ ref('week_trading_sessions') }}
                 where index = 0
                 group by symbol
             )
    select profile_id,
           collection_id,
           collection_uniq_id,
           datetime,
           '1y'::varchar as period,
           sum(open * weight)           as open,
           sum(high * weight)           as high,
           sum(low * weight)            as low,
           sum(close * weight)          as close,
           sum(adjusted_close * weight) as adjusted_close
    from {{ ref('historical_prices_aggregated_1d') }}
             join {{ ref('collection_tickers_weighted') }} using (symbol)
             left join latest_open_trading_session using (symbol)
    where historical_prices_aggregated_1d.datetime >= coalesce(latest_open_trading_session.date, now()) - interval '1 year'
    group by profile_id, collection_id, collection_uniq_id, datetime, period
)

union all

(
    with latest_open_trading_session as
             (
                 select symbol, max(date) as date
                 from {{ ref('week_trading_sessions') }}
                 where index = 0
                 group by symbol
             )
    select profile_id,
           collection_id,
           collection_uniq_id,
           datetime,
           '5y'::varchar as period,
           sum(open * weight)           as open,
           sum(high * weight)           as high,
           sum(low * weight)            as low,
           sum(close * weight)          as close,
           sum(adjusted_close * weight) as adjusted_close
    from {{ ref('historical_prices_aggregated_1w') }}
             join {{ ref('collection_tickers_weighted') }} using (symbol)
             left join latest_open_trading_session using (symbol)
    where historical_prices_aggregated_1w.datetime >= coalesce(latest_open_trading_session.date, now()) - interval '5 year'
    group by profile_id, collection_id, collection_uniq_id, datetime, period
)

union all

(
    select profile_id,
           collection_id,
           collection_uniq_id,
           datetime,
           'all'::varchar as period,
           sum(open * weight)           as open,
           sum(high * weight)           as high,
           sum(low * weight)            as low,
           sum(close * weight)          as close,
           sum(adjusted_close * weight) as adjusted_close
    from {{ ref('historical_prices_aggregated_1m') }}
             join {{ ref('collection_tickers_weighted') }} using (symbol)
    group by profile_id, collection_id, collection_uniq_id, datetime, period
)
