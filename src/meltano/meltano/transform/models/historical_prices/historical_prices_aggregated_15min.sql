{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('symbol, datetime'),
      index(this, 'id', true),
      'create index if not exists "datetime__symbol" ON {{ this }} (datetime, symbol)',
    ]
  )
}}


-- Execution Time: 140877.584 ms
with
{% if is_incremental() %}
     max_date as
         (
             select max(datetime) as datetime
             from {{ this }}
         ),
{% endif %}
     week_trading_sessions as
         (
             select min(open_at)  as open_at,
                    max(close_at) as close_at
             from {{ ref('exchange_schedule') }}
             where open_at between now() - interval '1 week' and now()
             group by date
         ),
     time_series_15min as
         (
             SELECT null as type,
                    time_15min::timestamp,
                    date
             FROM (
                      SELECT null as type,
                             date_trunc('minute', dd) -
                             interval '1 minute' *
                             mod(extract(minutes from dd)::int, 15) as time_15min,
                             dd::date as date
                      FROM generate_series(now()::timestamp - interval '1 week', now()::timestamp, interval '15 minutes') dd
                      ) t
                      join week_trading_sessions on true
{% if is_incremental() and var('realtime') %}
                      join max_date on true
{% endif %}
             where time_15min >= week_trading_sessions.open_at and time_15min < week_trading_sessions.close_at
{% if is_incremental() and var('realtime') %}
               and time_15min > max_date.datetime - interval '20 minutes'
{% endif %}
             union all
             SELECT 'crypto' as type,
                    (date_trunc('minute', dd) -
                     interval '1 minute' *
                     mod(extract(minutes from dd)::int, 15))::timestamp as time_15min,
                    dd::date                                            as date
             FROM generate_series(now()::timestamp - interval '1 week', now()::timestamp, interval '15 minutes') dd
{% if is_incremental() and var('realtime') %}
                      join max_date on true
             where dd > max_date.datetime - interval '20 minutes'
{% endif %}
         ),
     expanded_intraday_prices as
         (
             select t.symbol,
                    t.time_15min,
                    ip_open.open,
                    t.high,
                    t.low,
                    ip_close.close,
                    ip_close.adjusted_close,
                    t.volume,
                    t.updated_at
             from (
                      select symbol,
                             time_15min,
                             mode() within group ( order by time )      as open_time,
                             mode() within group ( order by time desc ) as close_time,
                             max(high)                                  as high,
                             min(low)                                   as low,
                             sum(volume)                                as volume,
                             max(updated_at)                            as updated_at
                      from (
                               select historical_intraday_prices.*
                               from {{ ref('historical_intraday_prices') }}
                                        join week_trading_sessions on true
{% if is_incremental() and var('realtime') %}
                                        join max_date on true
{% endif %}
                               where (historical_intraday_prices.time_15min >= week_trading_sessions.open_at - interval '1 hour' and historical_intraday_prices.time_15min < week_trading_sessions.close_at
                                  or (symbol like '%.CC' and time > now() - interval '1 week'))
{% if is_incremental() and var('realtime') %}
                                 and historical_intraday_prices.time_15min > max_date.datetime - interval '20 minutes'
{% endif %}
                           ) t
                      group by symbol, time_15min
                  ) t
                      join {{ ref('historical_intraday_prices') }} ip_open
                           on ip_open.symbol = t.symbol and ip_open.time = t.open_time
                      join {{ ref('historical_intraday_prices') }} ip_close
                           on ip_close.symbol = t.symbol and ip_close.time = t.close_time
         ),
     tickers_dates_skeleton as
         (
             select symbol,
                    null as open,
                    null as high,
                    null as low,
                    null as close,
                    null as volume,
                    time_15min,
                    date
             from {{ ref('base_tickers') }}
                      join time_series_15min
                           on (time_series_15min.type = 'crypto' and base_tickers.type = 'crypto')
                               or (time_series_15min.type is null and base_tickers.type != 'crypto')
             union all
             select contract_name,
                    null as open,
                    null as high,
                    null as low,
                    null as close,
                    null as volume,
                    time_15min,
                    date
             from {{ ref('ticker_options_monitored') }}
                      join time_series_15min on time_series_15min.type is null
         )
select tds.symbol || '_' || tds.time_15min                            as id,
       tds.symbol,
       tds.time_15min                                                 as datetime,
       expanded_intraday_prices.open::double precision,
       expanded_intraday_prices.high::double precision,
       expanded_intraday_prices.low::double precision,
       expanded_intraday_prices.close::double precision,
       coalesce(expanded_intraday_prices.adjusted_close,
                LAST_VALUE_IGNORENULLS(expanded_intraday_prices.adjusted_close) over (lookback),
                historical_prices_marked.price_1w
           )::double precision                                        as adjusted_close,
       coalesce(expanded_intraday_prices.volume, 0)::double precision as volume,
       expanded_intraday_prices.updated_at          as updated_at
from tickers_dates_skeleton tds
         left join expanded_intraday_prices using (symbol, time_15min)
         left join {{ ref('historical_prices_marked') }} using (symbol)
{% if is_incremental() %}
         left join {{ this }} old_data on old_data.symbol = tds.symbol and old_data.datetime = tds.time_15min
where old_data.symbol is null -- no old data
   or (expanded_intraday_prices.updated_at is not null and old_data.updated_at is null) -- old data is null and new is not
   or expanded_intraday_prices.updated_at > old_data.updated_at -- new data is newer than the old one
{% endif %}
    window
        lookback as (partition by tds.symbol order by tds.time_15min asc)

-- OK created incremental model historical_prices_aggregated_15min SELECT 3705058 in 183.20s
-- OK created incremental model historical_prices_aggregated_15min SELECT 3705058 in 186.82s
