{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists {{ get_index_name(this, "symbol__period__datetime") }} (symbol, period, datetime)',
      'create index if not exists {{ get_index_name(this, "period__datetime") }} (period, datetime)',
    ]
  )
}}

{% if is_incremental() %}
with max_date as
         (
             select symbol,
                    period,
                    max(time) as time
             from {{ this }}
             group by symbol, period
      )
{% endif %}

(
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
                 SELECT date_trunc('minute', dd) -
                        interval '1 minute' *
                        mod(extract(minutes from dd)::int, 3) as time_truncated
                 FROM generate_series(now()::timestamp - interval '1 week', now()::timestamp, interval '3 minutes') dd
                          join latest_open_trading_session on true
                 where dd between latest_open_trading_session.open_at and latest_open_trading_session.close_at
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
                 where eod_intraday_prices.time between latest_open_trading_session.open_at - interval '1 hour' and latest_open_trading_session.close_at
             ),
         combined_intraday_prices as
             (
                 select DISTINCT ON (
                     symbol,
                     time_truncated
                     ) (symbol || '_' || time_truncated || '_3min')::varchar                                                           as id,
                       symbol                                                                                                          as symbol,
                       time_truncated::timestamp                                                                                       as time, -- TODO remove
                       time_truncated::timestamp                                                                                       as datetime,
                       '3min'::varchar                                                                                                 as period,
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
{% if is_incremental() and var('realtime') %}
                          where time > now() - interval '20 minutes'
{% endif %}
                          union all
                          select symbol,
                                 time_truncated as time,
                                 null           as open,
                                 null           as high,
                                 null           as low,
                                 null           as close,
                                 null           as volume,
                                 time_truncated
                          from (select distinct symbol from expanded_intraday_prices) t1
                                   join time_series_3min on true
                          where not exists(select 1
                                           from expanded_intraday_prices
                                           where expanded_intraday_prices.symbol = t1.symbol
                                             and expanded_intraday_prices.time_truncated = time_series_3min.time_truncated)
                      ) t
                 order by symbol, time_truncated, time
             )
    select * from (
                      select (symbol || '_' || datetime || '_3min')::varchar as id,
                             symbol,
                             datetime                                        as time,
                             datetime                                        as datetime,
                             period,
                             coalesce(
                                     open,
                                     first_value(close)
                                     OVER (partition by symbol, grp order by datetime)
                                 )::double precision                         as open,
                             coalesce(
                                     high,
                                     first_value(close)
                                     OVER (partition by symbol, grp order by datetime)
                                 )::double precision                         as high,
                             coalesce(
                                     low,
                                     first_value(close)
                                     OVER (partition by symbol, grp order by datetime)
                                 )::double precision                         as low,
                             coalesce(
                                     close,
                                     first_value(close)
                                     OVER (partition by symbol, grp order by datetime)
                                 )::double precision                         as close,
                             coalesce(
                                     close,
                                     first_value(close)
                                     OVER (partition by symbol, grp order by datetime)
                                 )::double precision                         as adjusted_close,
                             coalesce(volume, 0)                             as volume
                      from (
                               select *,
                                      sum(case when close is not null then 1 end)
                                      over (partition by symbol order by datetime) as grp
                               from combined_intraday_prices
                           ) t
                  ) t2
    where t2.close is not null
)

union all

(
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
                 SELECT date_trunc('minute', dd) -
                        interval '1 minute' *
                        mod(extract(minutes from dd)::int, 15) as time_truncated
                 FROM generate_series(now()::timestamp - interval '1 week', now()::timestamp, interval '15 minutes') dd
                          join week_trading_sessions on true
                 where dd between week_trading_sessions.open_at and week_trading_sessions.close_at
{% if is_incremental() and var('realtime') %}
                   and dd > now() - interval '30 minutes'
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
                 where eod_intraday_prices.time between week_trading_sessions.open_at - interval '1 hour' and week_trading_sessions.close_at
             ),
         combined_intraday_prices as
             (
                 select DISTINCT ON (
                     symbol,
                     time_truncated
                     ) (symbol || '_' || time_truncated || '_15min')::varchar                                                          as id,
                       symbol                                                                                                          as symbol,
                       time_truncated::timestamp                                                                                       as time, -- TODO remove
                       time_truncated::timestamp                                                                                       as datetime,
                       '15min'::varchar                                                                                                as period,
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
{% if is_incremental() and var('realtime') %}
                          where time > now() - interval '30 minutes'
{% endif %}
                          union all
                          select symbol,
                                 time_truncated as time,
                                 null           as open,
                                 null           as high,
                                 null           as low,
                                 null           as close,
                                 null           as volume,
                                 time_truncated
                          from (select distinct symbol from expanded_intraday_prices) t1
                                   join time_series_15min on true
                          where not exists(select 1
                                           from expanded_intraday_prices
                                           where expanded_intraday_prices.symbol = t1.symbol
                                             and expanded_intraday_prices.time_truncated = time_series_15min.time_truncated)
                      ) t
                 order by symbol, time_truncated, time
             )
    select *
    from (
             select (symbol || '_' || datetime || '_15min')::varchar as id,
                    symbol,
                    datetime                                         as time,
                    datetime                                         as datetime,
                    period,
                    coalesce(
                            open,
                            first_value(close)
                            OVER (partition by symbol, grp order by datetime)
                        )::double precision                          as open,
                    coalesce(
                            high,
                            first_value(close)
                            OVER (partition by symbol, grp order by datetime)
                        )::double precision                          as high,
                    coalesce(
                            low,
                            first_value(close)
                            OVER (partition by symbol, grp order by datetime)
                        )::double precision                          as low,
                    coalesce(
                            close,
                            first_value(close)
                            OVER (partition by symbol, grp order by datetime)
                        )::double precision                          as close,
                    coalesce(
                            close,
                            first_value(close)
                            OVER (partition by symbol, grp order by datetime)
                        )::double precision                          as adjusted_close,
                    coalesce(volume, 0)                              as volume
             from (
                      select *,
                             sum(case when close is not null then 1 end)
                             over (partition by symbol order by datetime) as grp
                      from combined_intraday_prices
                  ) t
         ) t2
    where t2.close is not null
)

{% if not var('realtime') %}

union all

(
    with time_series_1d as
        (
            SELECT distinct date::timestamp as datetime
            FROM {{ ref('historical_prices') }}
        )
    select distinct on (
        code, time_series_1d.datetime
        ) (code || '_' || time_series_1d.datetime::date || '_1d')::varchar                    as id,
         code                                                                                 as symbol,
         time_series_1d.datetime                                                              as time,
         time_series_1d.datetime                                                              as datetime,
         '1d'::varchar                                                                        as period,
         case
             when historical_prices.date = time_series_1d.datetime then historical_prices.open
             else historical_prices.close end                                                 as open,
         case
             when historical_prices.date = time_series_1d.datetime then historical_prices.high
             else historical_prices.close end                                                 as high,
         case
             when historical_prices.date = time_series_1d.datetime then historical_prices.low
             else historical_prices.close end                                                 as low,
         case
             when historical_prices.date = time_series_1d.datetime then historical_prices.close
             else historical_prices.close end                                                 as close,
         case
             when historical_prices.date = time_series_1d.datetime
                 then historical_prices.adjusted_close
             else historical_prices.adjusted_close end                                        as adjusted_close,
         case
             when historical_prices.date = time_series_1d.datetime then historical_prices.volume
             else 0.0 end                                                                            as volume
    from {{ ref('historical_prices') }}
             join time_series_1d on time_series_1d.datetime between historical_prices.date and historical_prices.date + interval '1 week'
    {% if is_incremental() %}
        left join max_date on max_date.symbol = code and max_date.period = '1d'
        where max_date.time is null or date >= max_date.time - interval '1 week'
    {% endif %}
    order by code, time_series_1d.datetime, date desc
)

union all

(
    with time_series_1w as
             (
                 SELECT distinct date_trunc('week', date)::timestamp as datetime
                 FROM {{ ref('historical_prices') }}
             )

    select distinct on (
        symbol, time_series_1w.datetime
        ) (symbol || '_' || time_series_1w.datetime::timestamptz || '_1w')::varchar as id,
          symbol,
          time_series_1w.datetime                                                   as time,
          time_series_1w.datetime                                                   as datetime,
          period,
          case
              when historical_prices.datetime = time_series_1w.datetime then historical_prices.open
              else historical_prices.close end                                      as open,
          case
              when historical_prices.datetime = time_series_1w.datetime then historical_prices.high
              else historical_prices.close end                                      as high,
          case
              when historical_prices.datetime = time_series_1w.datetime then historical_prices.low
              else historical_prices.close end                                      as low,
          case
              when historical_prices.datetime = time_series_1w.datetime then historical_prices.close
              else historical_prices.close end                                      as close,
          case
              when historical_prices.datetime = time_series_1w.datetime
                  then historical_prices.adjusted_close
              else historical_prices.adjusted_close end                             as adjusted_close,
          case
              when historical_prices.datetime = time_series_1w.datetime then historical_prices.volume
              else 0.0 end                                                          as volume
    from (
             select DISTINCT ON (
                 code,
                 date_trunc('week', date)
                 ) code                                                                                                              as symbol,
                   date_trunc('week', date)::timestamp                                                                               as time,
                   date_trunc('week', date)::timestamp                                                                               as datetime,
                   '1w'::varchar                                                                                                     as period,
                   first_value(open::double precision)
                   OVER (partition by code, date_trunc('week', date) order by date rows between current row and unbounded following) as open,
                   max(high::double precision)
                   OVER (partition by code, date_trunc('week', date) rows between current row and unbounded following)               as high,
                   min(low::double precision)
                   OVER (partition by code, date_trunc('week', date) rows between current row and unbounded following)               as low,
                   last_value(close::double precision)
                   OVER (partition by code, date_trunc('week', date) order by date rows between current row and unbounded following) as close,
                   last_value(adjusted_close::double precision)
                   OVER (partition by code, date_trunc('week', date) order by date rows between current row and unbounded following) as adjusted_close,
                   (sum(volume::numeric)
                    OVER (partition by code, date_trunc('week', date)))::double precision                                            as volume
             from {{ ref('historical_prices') }}
             {% if is_incremental() %}
                 left join max_date on max_date.symbol = code and max_date.period = '1w'
                 where max_date.time is null or date >= max_date.time
             {% endif %}
             order by code, date_trunc('week', date), date
         ) historical_prices
             join time_series_1w
                  on time_series_1w.datetime between historical_prices.datetime and historical_prices.datetime + interval '1 month'
    order by symbol, time_series_1w.datetime, historical_prices.datetime desc
)

union all

(
    with time_series_1m as
             (
                 SELECT distinct date_trunc('month', date)::timestamp as datetime
                 FROM {{ ref('historical_prices') }}
             )

    select distinct on (
        symbol, time_series_1m.datetime
        ) (symbol || '_' || time_series_1m.datetime::timestamptz || '_1m')::varchar as id,
          symbol,
          time_series_1m.datetime                                                   as time,
          time_series_1m.datetime                                                   as datetime,
          period,
          case
              when historical_prices.datetime = time_series_1m.datetime then historical_prices.open
              else historical_prices.close end                                      as open,
          case
              when historical_prices.datetime = time_series_1m.datetime then historical_prices.high
              else historical_prices.close end                                      as high,
          case
              when historical_prices.datetime = time_series_1m.datetime then historical_prices.low
              else historical_prices.close end                                      as low,
          case
              when historical_prices.datetime = time_series_1m.datetime then historical_prices.close
              else historical_prices.close end                                      as close,
          case
              when historical_prices.datetime = time_series_1m.datetime
                  then historical_prices.adjusted_close
              else historical_prices.adjusted_close end                             as adjusted_close,
          case
              when historical_prices.datetime = time_series_1m.datetime then historical_prices.volume
              else 0.0 end                                                          as volume
    from (
             select DISTINCT ON (
                 code,
                 date_trunc('month', date)
                 ) code                                                                                                               as symbol,
                   date_trunc('month', date)::timestamp                                                                               as time,
                   date_trunc('month', date)::timestamp                                                                               as datetime,
                   '1m'::varchar                                                                                                      as period,
                   first_value(open::double precision)
                   OVER (partition by code, date_trunc('month', date) order by date rows between current row and unbounded following) as open,
                   max(high::double precision)
                   OVER (partition by code, date_trunc('month', date) rows between current row and unbounded following)               as high,
                   min(low::double precision)
                   OVER (partition by code, date_trunc('month', date) rows between current row and unbounded following)               as low,
                   last_value(close::double precision)
                   OVER (partition by code, date_trunc('month', date) order by date rows between current row and unbounded following) as close,
                   last_value(adjusted_close::double precision)
                   OVER (partition by code, date_trunc('month', date) order by date rows between current row and unbounded following) as adjusted_close,
                   (sum(volume::numeric)
                    OVER (partition by code, date_trunc('month', date)))::double precision                                            as volume
             from {{ ref('historical_prices') }}
             {% if is_incremental() %}
                 left join max_date on max_date.symbol = code and max_date.period = '1m'
                 where max_date.time is null or date >= max_date.time
             {% endif %}
             order by code, date_trunc('month', date), date
         ) historical_prices
             join time_series_1m
                  on time_series_1m.datetime between historical_prices.datetime and historical_prices.datetime + interval '1 month'
    order by symbol, time_series_1m.datetime, historical_prices.datetime desc
)
{% endif %}