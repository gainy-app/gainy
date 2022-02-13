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
    with combined_daily_prices as
             (
                 (
                     select code as symbol,
                            date as datetime,
                            open,
                            high,
                            low,
                            close,
                            adjusted_close,
                            volume
                     from {{ ref('historical_prices') }}
                     where date >= now() - interval '1 year' - interval '1 week'
                 )
                 union
                 (
                     with time_series_1d as
                              (
                                  SELECT distinct date
                                  FROM historical_prices
                                  where date >= now() - interval '1 year' - interval '1 week'
                              )
                     select code                   as symbol,
                            time_series_1d.date    as datetime,
                            null::double precision as open,
                            null::double precision as high,
                            null::double precision as low,
                            null::double precision as close,
                            null::double precision as volume,
                            null::double precision as adjusted_close
                     from (select distinct code from historical_prices) t1
                              join time_series_1d on true
                     where not exists(select 1
                                      from historical_prices
                                      where historical_prices.code = t1.code
                                        and historical_prices.date = time_series_1d.date)
                 )
             )
    select *
    from (
             select (symbol || '_' || datetime || '_1d')::varchar as id,
                    symbol,
                    datetime::timestamp                           as time,
                    datetime::timestamp                           as datetime,
                    '1d'::varchar                                 as period,
                    coalesce(
                            open,
                            first_value(close)
                            OVER (partition by symbol, grp order by datetime)
                        )::double precision                       as open,
                    coalesce(
                            high,
                            first_value(close)
                            OVER (partition by symbol, grp order by datetime)
                        )::double precision                       as high,
                    coalesce(
                            low,
                            first_value(close)
                            OVER (partition by symbol, grp order by datetime)
                        )::double precision                       as low,
                    coalesce(
                            close,
                            first_value(close)
                            OVER (partition by symbol, grp order by datetime)
                        )::double precision                       as close,
                    coalesce(
                            close,
                            first_value(close)
                            OVER (partition by symbol, grp order by datetime)
                        )::double precision                       as adjusted_close,
                    coalesce(volume, 0)::double precision         as volume
             from (
                      select combined_daily_prices.*,
                             sum(case when close is not null then 1 end)
                             over (partition by combined_daily_prices.symbol order by datetime) as grp
                      from combined_daily_prices
{% if is_incremental() %}
                      left join max_date on max_date.symbol = combined_daily_prices.symbol and max_date.period = '1d'
                      where (max_date.time is null or combined_daily_prices.datetime >= max_date.time - interval '1 week')
{% endif %}
                  ) t
         ) t2
    where t2.close is not null
)

union all

(
    select DISTINCT ON (
        code,
        date_truncated
        ) (code || '_' || date_truncated || '_1w')::varchar as id,
          code as symbol,
          date_truncated::timestamp                                                                                     as time,
          date_truncated::timestamp                                                                                     as datetime,
          '1w'::varchar                                                                                                 as period,
          first_value(open::double precision)
          OVER (partition by code, date_truncated order by date rows between current row and unbounded following)       as open,
          max(high::double precision)
          OVER (partition by code, date_truncated rows between current row and unbounded following)                     as high,
          min(low::double precision)
          OVER (partition by code, date_truncated rows between current row and unbounded following)                     as low,
          last_value(close::double precision)
          OVER (partition by code, date_truncated order by date rows between current row and unbounded following)       as close,
          last_value(adjusted_close::double precision)
          OVER (partition by code, date_truncated order by date rows between current row and unbounded following)       as adjusted_close,
          (sum(volume::numeric)
           OVER (partition by code, date_truncated rows between current row and unbounded following))::double precision as volume
    from (
             select historical_prices.*,
                    date_trunc('week', date) as date_truncated
             from {{ ref('historical_prices') }}
{% if is_incremental() %}
             left join max_date on max_date.symbol = code and max_date.period = '1w'
             where (max_date.time is null or date_trunc('week', date) >= max_date.time - interval '1 month')
               and date >= now() - interval '5 year' - interval '1 week'
{% else %}
             where date >= now() - interval '5 year' - interval '1 week'
{% endif %}
         ) t
    order by symbol, date_truncated, date
)

union all

(
    select DISTINCT ON (
        code,
        date_truncated
        ) (code || '_' || date_truncated || '_1m')::varchar as id,
          code as symbol,
          date_truncated::timestamp                                                                                     as time,
          date_truncated::timestamp                                                                                     as datetime,
          '1m'::varchar                                                                                                 as period,
          first_value(open::double precision)
          OVER (partition by code, date_truncated order by date rows between current row and unbounded following)       as open,
          max(high::double precision)
          OVER (partition by code, date_truncated rows between current row and unbounded following)                     as high,
          min(low::double precision)
          OVER (partition by code, date_truncated rows between current row and unbounded following)                     as low,
          last_value(close::double precision)
          OVER (partition by code, date_truncated order by date rows between current row and unbounded following)       as close,
          last_value(adjusted_close::double precision)
          OVER (partition by code, date_truncated order by date rows between current row and unbounded following)       as adjusted_close,
          (sum(volume::numeric)
           OVER (partition by code, date_truncated rows between current row and unbounded following))::double precision as volume
    from (
             select historical_prices.*,
                    date_trunc('month', date) as date_truncated
             from {{ ref('historical_prices') }}
{% if is_incremental() %}
             left join max_date on max_date.symbol = code and max_date.period = '1m'
             where (max_date.time is null or date_trunc('month', date) >= max_date.time - interval '3 month')
{% endif %}
         ) t
    order by symbol, date_truncated, date
)
{% endif %}