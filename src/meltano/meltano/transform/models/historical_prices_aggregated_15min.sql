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

with week_trading_sessions as
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
                    date_trunc('minute', dd) -
                    interval '1 minute' *
                    mod(extract(minutes from dd)::int, 15) as time_truncated
             FROM generate_series(now()::timestamp - interval '1 week', now()::timestamp, interval '15 minutes') dd
                      join week_trading_sessions on true
             where dd between week_trading_sessions.open_at and week_trading_sessions.close_at
{% if is_incremental() and var('realtime') %}
               and dd > now() - interval '1 hour'
{% endif %}
             union all
             SELECT 'crypto' as type,
                    date_trunc('minute', dd) -
                    interval '1 minute' *
                    mod(extract(minutes from dd)::int, 15) as time_truncated
             FROM generate_series(now()::timestamp - interval '1 week', now()::timestamp, interval '15 minutes') dd
{% if is_incremental() and var('realtime') %}
             where dd > now() - interval '1 hour'
{% endif %}
         ),
     expanded_intraday_prices as
         (
             select eod_intraday_prices.*,
                    date_trunc('minute', eod_intraday_prices.time) -
                    interval '1 minute' *
                    mod(extract(minutes from eod_intraday_prices.time)::int, 15) as time_truncated
             from {{ source('eod', 'eod_intraday_prices') }}
                      join week_trading_sessions on true
             where (eod_intraday_prices.time between week_trading_sessions.open_at - interval '1 hour' and week_trading_sessions.close_at
                or (symbol like '%.CC' and time > now() - interval '1 week'))
{% if is_incremental() and var('realtime') %}
               and eod_intraday_prices.time > now() - interval '1 hour'
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
                      from {{ ref('base_tickers') }}
                               join time_series_15min
                                    on (time_series_15min.type = 'crypto' and base_tickers.type = 'crypto')
                                        or (time_series_15min.type is null and base_tickers.type != 'crypto')
                      where not exists(select 1
                                       from expanded_intraday_prices
                                       where expanded_intraday_prices.symbol = base_tickers.symbol
                                         and expanded_intraday_prices.time_truncated = time_series_15min.time_truncated)
                  ) t
             order by symbol, time_truncated, time
         )
select *
from (
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
