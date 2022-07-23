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
           datetime,
           '1w'::varchar as period,
           sum(open * weight)           as open,
           sum(high * weight)           as high,
           sum(low * weight)            as low,
           sum(close * weight)          as close,
           sum(adjusted_close * weight) as adjusted_close
    from {{ ref('historical_prices_aggregated_15min') }}
             join {{ ref('collection_tickers_weighted') }} using (symbol)
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
           datetime,
           '1m'::varchar as period,
           sum(open * weight)           as open,
           sum(high * weight)           as high,
           sum(low * weight)            as low,
           sum(close * weight)          as close,
           sum(adjusted_close * weight) as adjusted_close
    from {{ ref('historical_prices_aggregated_1d') }}
             join {{ ref('collection_tickers_weighted') }} using (symbol)
             left join {{ ref('week_trading_sessions') }}
                       on week_trading_sessions.symbol = historical_prices_aggregated_1d.symbol
                           and week_trading_sessions.index = 0
    where historical_prices_aggregated_1d.datetime >= coalesce(week_trading_sessions.date, now()) - interval '1 month'
    group by profile_id, collection_id, collection_uniq_id, datetime, period
)

union all

(
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
             left join {{ ref('week_trading_sessions') }}
                       on week_trading_sessions.symbol = historical_prices_aggregated_1d.symbol
                           and week_trading_sessions.index = 0
    where historical_prices_aggregated_1d.datetime >= coalesce(week_trading_sessions.date, now()) - interval '3 month'
    group by profile_id, collection_id, collection_uniq_id, datetime, period
)

union all

(
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
             left join {{ ref('week_trading_sessions') }}
                       on week_trading_sessions.symbol = historical_prices_aggregated_1d.symbol
                           and week_trading_sessions.index = 0
    where historical_prices_aggregated_1d.datetime >= coalesce(week_trading_sessions.date, now()) - interval '1 year'
    group by profile_id, collection_id, collection_uniq_id, datetime, period
)

union all

(
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
             left join {{ ref('week_trading_sessions') }}
                       on week_trading_sessions.symbol = historical_prices_aggregated_1w.symbol
                           and week_trading_sessions.index = 0
    where historical_prices_aggregated_1w.datetime >= coalesce(week_trading_sessions.date, now()) - interval '5 year'
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
