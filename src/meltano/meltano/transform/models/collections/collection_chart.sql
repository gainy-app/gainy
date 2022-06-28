{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}


with latest_open_trading_session as (
    select distinct on (exchange_name) *
    from {{ ref('exchange_schedule') }}
    where open_at <= now()
    order by exchange_name, date desc
)

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
             join {{ ref('base_tickers') }} on base_tickers.symbol = historical_prices_aggregated_3min.symbol
             left join latest_open_trading_session
                       on (latest_open_trading_session.exchange_name = base_tickers.exchange_canonical or
                           (base_tickers.exchange_canonical is null and
                            latest_open_trading_session.country_name = base_tickers.country_name))
                      and latest_open_trading_session.date = historical_prices_aggregated_3min.datetime::date
    where (historical_prices_aggregated_3min.datetime between latest_open_trading_session.open_at and latest_open_trading_session.close_at
       or (base_tickers.type = 'crypto' and historical_prices_aggregated_3min.datetime > now() - interval '1 day'))
    group by profile_id, collection_id, collection_uniq_id, datetime, period
)

union all

(
    with week_trading_sessions as (
        select *
        from {{ ref('exchange_schedule') }}
        where open_at between now() - interval '1 week' and now()
    )
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
             join {{ ref('base_tickers') }} on base_tickers.symbol = historical_prices_aggregated_15min.symbol
             left join week_trading_sessions
                       on (week_trading_sessions.exchange_name = base_tickers.exchange_canonical or
                           (base_tickers.exchange_canonical is null and
                            week_trading_sessions.country_name = base_tickers.country_name))
                      and week_trading_sessions.date = historical_prices_aggregated_15min.datetime::date
    where (historical_prices_aggregated_15min.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at
       or (base_tickers.type = 'crypto' and historical_prices_aggregated_15min.datetime > now() - interval '7 days'))
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
             join {{ ref('base_tickers') }} on base_tickers.symbol = historical_prices_aggregated_1d.symbol
             left join latest_open_trading_session
                       on (latest_open_trading_session.exchange_name = base_tickers.exchange_canonical or
                           (base_tickers.exchange_canonical is null and
                            latest_open_trading_session.country_name = base_tickers.country_name))
    where historical_prices_aggregated_1d.datetime >= coalesce(latest_open_trading_session.date, now()) - interval '1 month'
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
             join {{ ref('base_tickers') }} on base_tickers.symbol = historical_prices_aggregated_1d.symbol
             left join latest_open_trading_session
                       on (latest_open_trading_session.exchange_name = base_tickers.exchange_canonical or
                           (base_tickers.exchange_canonical is null and
                            latest_open_trading_session.country_name = base_tickers.country_name))
    where historical_prices_aggregated_1d.datetime >= coalesce(latest_open_trading_session.date, now()) - interval '3 month'
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
             join {{ ref('base_tickers') }} on base_tickers.symbol = historical_prices_aggregated_1d.symbol
             left join latest_open_trading_session
                       on (latest_open_trading_session.exchange_name = base_tickers.exchange_canonical or
                           (base_tickers.exchange_canonical is null and
                            latest_open_trading_session.country_name = base_tickers.country_name))
    where historical_prices_aggregated_1d.datetime >= coalesce(latest_open_trading_session.date, now()) - interval '1 year'
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
             join {{ ref('base_tickers') }} on base_tickers.symbol = historical_prices_aggregated_1w.symbol
             left join latest_open_trading_session
                       on (latest_open_trading_session.exchange_name = base_tickers.exchange_canonical or
                           (base_tickers.exchange_canonical is null and
                            latest_open_trading_session.country_name = base_tickers.country_name))
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
