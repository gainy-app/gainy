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


with
{% if is_incremental() %}
     max_date as
         (
             select max(datetime) as datetime
             from {{ this }}
         ),
{% endif %}
     latest_open_trading_session as
         (
             select min(open_at)::timestamp  as open_at,
                    max(close_at)::timestamp as close_at
             from (
                      select distinct on (exchange_name) *
                      from {{ ref('exchange_schedule') }}
                      where open_at <= now()
                      order by exchange_name, date desc
                  ) t
         ),
     time_series_3min as
         (
             SELECT null as type,
                    time_3min
             FROM (
                      SELECT null as type,
                             date_trunc('minute', dd) -
                             interval '1 minute' *
                             mod(extract(minutes from dd)::int, 3) as time_3min
                      FROM generate_series(now()::timestamp - interval '1 week', now()::timestamp, interval '3 minutes') dd
                      ) t
                      join latest_open_trading_session on true
{% if is_incremental() and var('realtime') %}
                      join max_date on true
{% endif %}
             where time_3min >= latest_open_trading_session.open_at and time_3min < latest_open_trading_session.close_at
{% if is_incremental() and var('realtime') %}
               and time_3min > max_date.datetime - interval '20 minutes'
{% endif %}
             union all
             SELECT 'crypto' as type,
                    date_trunc('minute', dd) -
                    interval '1 minute' *
                    mod(extract(minutes from dd)::int, 3) as time_3min
             FROM generate_series(now()::timestamp - interval '1 day', now()::timestamp, interval '3 minutes') dd
{% if is_incremental() and var('realtime') %}
                      join max_date on true
             where dd > max_date.datetime - interval '20 minutes'
{% endif %}
         ),
     expanded_intraday_prices as
         (
             select historical_intraday_prices.*
             from {{ ref('historical_intraday_prices') }}
                      join latest_open_trading_session on true
{% if is_incremental() and var('realtime') %}
                      join max_date on true
{% endif %}
             where (historical_intraday_prices.time_3min >= latest_open_trading_session.open_at - interval '1 hour' and historical_intraday_prices.time_3min < latest_open_trading_session.close_at
                or (symbol like '%.CC' and time > now() - interval '1 day'))
{% if is_incremental() and var('realtime') %}
               and historical_intraday_prices.time_3min > max_date.datetime - interval '20 minutes'
{% endif %}
         ),
     combined_intraday_prices as
         (
             select DISTINCT ON (
                 symbol,
                 time_3min
                 ) symbol                                                                                                     as symbol,
                   time_3min::timestamp                                                                                       as datetime,
                   first_value(open::double precision)
                   OVER (partition by symbol, time_3min order by time rows between current row and unbounded following)       as open,
                   max(high::double precision)
                   OVER (partition by symbol, time_3min rows between current row and unbounded following)                     as high,
                   min(low::double precision)
                   OVER (partition by symbol, time_3min rows between current row and unbounded following)                     as low,
                   last_value(close::double precision)
                   OVER (partition by symbol, time_3min order by time rows between current row and unbounded following)       as close,
                   (sum(volume::numeric)
                    OVER (partition by symbol, time_3min rows between current row and unbounded following))::double precision as volume
             from (
                      select symbol,
                             time,
                             open,
                             high,
                             low,
                             close,
                             volume,
                             time_3min,
                             0 as priority
                      from expanded_intraday_prices
                      union all
                      select symbol,
                             time_3min as time,
                             null           as open,
                             null           as high,
                             null           as low,
                             null           as close,
                             null           as volume,
                             time_3min,
                             1 as priority
                      from {{ ref('base_tickers') }}
                               join time_series_3min
                                   on (time_series_3min.type = 'crypto' and base_tickers.type = 'crypto')
                                       or (time_series_3min.type is null and base_tickers.type != 'crypto')
                      union all
                      select contract_name,
                             time_3min as time,
                             null           as open,
                             null           as high,
                             null           as low,
                             null           as close,
                             null           as volume,
                             time_3min,
                             1              as priority
                      from {{ ref('ticker_options_monitored') }}
                               join time_series_3min on time_series_3min.type is null
                  ) t
             order by symbol, time_3min, time, priority
         )
select * from (
                  select symbol || '_' || datetime as id,
                         symbol,
                         datetime                  as datetime,
                         coalesce(
                                 open,
                                 first_value(close)
                                 OVER (partition by symbol, grp order by datetime),
                                 historical_prices_marked.price_0d
                             )::double precision   as open,
                         coalesce(
                                 high,
                                 first_value(close)
                                 OVER (partition by symbol, grp order by datetime),
                                 historical_prices_marked.price_0d
                             )::double precision   as high,
                         coalesce(
                                 low,
                                 first_value(close)
                                 OVER (partition by symbol, grp order by datetime),
                                 historical_prices_marked.price_0d
                             )::double precision   as low,
                         coalesce(
                                 close,
                                 first_value(close)
                                 OVER (partition by symbol, grp order by datetime),
                                 historical_prices_marked.price_0d
                             )::double precision   as close,
                         coalesce(
                                 close,
                                 first_value(close)
                                 OVER (partition by symbol, grp order by datetime),
                                 historical_prices_marked.price_0d
                             )::double precision   as adjusted_close,
                         coalesce(volume, 0)       as volume
                  from (
                           select *,
                                  coalesce(sum(case when close is not null then 1 end)
                                           over (partition by symbol order by datetime), 0) as grp
                           from combined_intraday_prices
                       ) t
                           left join {{ ref('historical_prices_marked') }} using (symbol)
            ) t2
where t2.close is not null

-- Execution Time: 18457.714 ms on test
-- OK created incremental model historical_prices_aggregated_3min SELECT 3397613 in 71.67s
-- OK created incremental model historical_prices_aggregated_3min SELECT 3397613 in 127.68s
