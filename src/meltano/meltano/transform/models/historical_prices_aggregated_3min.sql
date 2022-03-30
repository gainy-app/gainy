{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists "symbol__datetime" ON {{ this }} (symbol, datetime)',
    ]
  )
}}

with latest_open_trading_session as
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
                    date_trunc('minute', dd) -
                    interval '1 minute' *
                    mod(extract(minutes from dd)::int, 3) as time_truncated
             FROM generate_series(now()::timestamp - interval '1 week', now()::timestamp, interval '3 minutes') dd
                      join latest_open_trading_session on true
             where dd between latest_open_trading_session.open_at and latest_open_trading_session.close_at
{% if is_incremental() and var('realtime') %}
             and dd > now() - interval '20 minutes'
{% endif %}
             union all
             SELECT 'crypto' as type,
                    date_trunc('minute', dd) -
                    interval '1 minute' *
                    mod(extract(minutes from dd)::int, 3) as time_truncated
             FROM generate_series(now()::timestamp - interval '1 day', now()::timestamp, interval '3 minutes') dd
{% if is_incremental() and var('realtime') %}
             and dd > now() - interval '20 minutes'
{% endif %}
         ),
     expanded_intraday_prices as
         (
             select eod_intraday_prices.*,
                    date_trunc('minute', eod_intraday_prices.time) -
                    interval '1 minute' *
                    mod(extract(minutes from eod_intraday_prices.time)::int, 3) as time_truncated
             from {{ source('eod', 'eod_intraday_prices') }}
                      join latest_open_trading_session on true
             where (eod_intraday_prices.time between latest_open_trading_session.open_at - interval '1 hour' and latest_open_trading_session.close_at
                or (symbol like '%.CC' and time > now() - interval '1 day'))
{% if is_incremental() and var('realtime') %}
               and eod_intraday_prices.time > now() - interval '20 minutes'
{% endif %}
         ),
     combined_intraday_prices as
         (
             select DISTINCT ON (
                 symbol,
                 time_truncated
                 ) symbol                                                                                                          as symbol,
                   time_truncated::timestamp                                                                                       as time, -- TODO remove
                   time_truncated::timestamp                                                                                       as datetime,
                   first_value(open::double precision)
                   OVER (partition by symbol, time_truncated order by time rows between current row and unbounded following)       as open,
                   max(high::double precision)
                   OVER (partition by symbol, time_truncated rows between current row and unbounded following)                     as high,
                   min(low::double precision)
                   OVER (partition by symbol, time_truncated rows between current row and unbounded following)                     as low,
                   last_value(close::double precision)
                   OVER (partition by symbol, time_truncated order by time rows between current row and unbounded following)       as close,
                   (sum(volume::numeric)
                    OVER (partition by symbol, time_truncated rows between current row and unbounded following))::double precision as volume
             from (
                      select symbol,
                             time,
                             open,
                             high,
                             low,
                             close,
                             volume,
                             time_truncated
                      from expanded_intraday_prices
                      union all
                      select symbol,
                             time_truncated as time,
                             null           as open,
                             null           as high,
                             null           as low,
                             null           as close,
                             null           as volume,
                             time_truncated
                      from base_tickers
                               join time_series_3min
                                   on (time_series_3min.type = 'crypto' and base_tickers.type = 'crypto')
                                   or (time_series_3min.type is null and base_tickers.type != 'crypto')
                      where not exists(select 1
                                       from expanded_intraday_prices
                                       where expanded_intraday_prices.symbol = base_tickers.symbol
                                         and expanded_intraday_prices.time_truncated = time_series_3min.time_truncated)
                  ) t
             order by symbol, time_truncated, time
         )
select * from (
                  select (symbol || '_' || datetime)::varchar as id,
                         symbol,
                         datetime                             as datetime,
                         coalesce(
                                 open,
                                 first_value(close)
                                 OVER (partition by symbol, grp order by datetime)
                             )::double precision              as open,
                         coalesce(
                                 high,
                                 first_value(close)
                                 OVER (partition by symbol, grp order by datetime)
                             )::double precision              as high,
                         coalesce(
                                 low,
                                 first_value(close)
                                 OVER (partition by symbol, grp order by datetime)
                             )::double precision              as low,
                         coalesce(
                                 close,
                                 first_value(close)
                                 OVER (partition by symbol, grp order by datetime)
                             )::double precision              as close,
                         coalesce(
                                 close,
                                 first_value(close)
                                 OVER (partition by symbol, grp order by datetime)
                             )::double precision              as adjusted_close,
                         coalesce(volume, 0)                  as volume
                  from (
                           select *,
                                  sum(case when close is not null then 1 end)
                                  over (partition by symbol order by datetime) as grp
                           from combined_intraday_prices
                       ) t
              ) t2
where t2.close is not null
