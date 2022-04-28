{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('symbol, period, datetime'),
      index(this, 'id', true),
      'create index if not exists "period__datetime" ON {{ this }} (period, datetime)',
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

-- 3min
-- Execution Time: 18457.714 ms on test
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
                 SELECT null as type,
                        time_truncated
                 FROM (
                          SELECT null as type,
                                 date_trunc('minute', dd) -
                                 interval '1 minute' *
                                 mod(extract(minutes from dd)::int, 3) as time_truncated
                          FROM generate_series(now()::timestamp - interval '1 week', now()::timestamp, interval '3 minutes') dd
                          ) t
                          join latest_open_trading_session on true
                 where time_truncated between latest_open_trading_session.open_at and latest_open_trading_session.close_at
{% if is_incremental() and var('realtime') %}
                   and time_truncated > now() - interval '20 minutes'
{% endif %}
                 union all
                 SELECT 'crypto' as type,
                        date_trunc('minute', dd) -
                        interval '1 minute' *
                        mod(extract(minutes from dd)::int, 3) as time_truncated
                 FROM generate_series(now()::timestamp - interval '1 day', now()::timestamp, interval '3 minutes') dd
{% if is_incremental() and var('realtime') %}
                 where dd > now() - interval '20 minutes'
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
                                 time_truncated,
                                 0 as priority
                          from expanded_intraday_prices
                          union all
                          select symbol,
                                 time_truncated as time,
                                 null           as open,
                                 null           as high,
                                 null           as low,
                                 null           as close,
                                 null           as volume,
                                 time_truncated,
                                 1 as priority
                          from {{ ref('base_tickers') }}
                                   join time_series_3min
                                       on (time_series_3min.type = 'crypto' and base_tickers.type = 'crypto')
                                           or (time_series_3min.type is null and base_tickers.type != 'crypto')
                      ) t
                 order by symbol, time_truncated, time, priority
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

-- 15min
-- Execution Time: 314494.955 ms on test
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
                 SELECT null as type,
                        time_truncated
                 FROM (
                          SELECT null as type,
                                 date_trunc('minute', dd) -
                                 interval '1 minute' *
                                 mod(extract(minutes from dd)::int, 15) as time_truncated
                          FROM generate_series(now()::timestamp - interval '1 week', now()::timestamp, interval '15 minutes') dd
                          ) t
                          join week_trading_sessions on true
                 where time_truncated between week_trading_sessions.open_at and week_trading_sessions.close_at
{% if is_incremental() and var('realtime') %}
                   and time_truncated > now() - interval '1 hour'
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
                                 time_truncated,
                                 0 as priority
                          from expanded_intraday_prices
                          union all
                          select symbol,
                                 time_truncated as time,
                                 null           as open,
                                 null           as high,
                                 null           as low,
                                 null           as close,
                                 null           as volume,
                                 time_truncated,
                                 1 as priority
                          from {{ ref('base_tickers') }}
                                   join time_series_15min
                                        on (time_series_15min.type = 'crypto' and base_tickers.type = 'crypto')
                                            or (time_series_15min.type is null and base_tickers.type != 'crypto')
                      ) t
                 order by symbol, time_truncated, time, priority
             )
    select id,
           symbol,
           time,
           datetime,
           period,
           open,
           high,
           low,
           close,
           (case
                when adjustment_rate is null or adjustment_rate2 is null or abs(close) < 1e-3
                    then 1
                when adjustment_rate = adjustment_rate2 or abs(daily_adjusted_close / close - adjustment_rate) <
                                                           abs(daily_adjusted_close / close - adjustment_rate2)
                    then adjustment_rate
                else adjustment_rate2
                end * close
               )::double precision as adjusted_close,
           volume
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
                    coalesce(volume, 0)                              as volume,
                    adjustment_rate,
                    first_value(adjustment_rate)
                    OVER (partition by symbol order by datetime)     as adjustment_rate2,
                    daily_adjusted_close
             from (
                      select combined_intraday_prices.*,
                             sum(case when combined_intraday_prices.close is not null then 1 end)
                             over (partition by combined_intraday_prices.symbol order by datetime)        as grp,
                             case
                                 when historical_prices.close > 0
                                     then historical_prices.adjusted_close::numeric / historical_prices.close::numeric
                             end as adjustment_rate,
                             historical_prices.adjusted_close::numeric as daily_adjusted_close
                      from combined_intraday_prices
                               left join {{ ref('historical_prices') }}
                                   on historical_prices.code = combined_intraday_prices.symbol
                                    and historical_prices.date = combined_intraday_prices.datetime::date
                  ) t
         ) t2
    where t2.close is not null
)

{% if not var('realtime') %}

union all

-- 1d
-- Execution Time: 90778.395 ms on test
(
    with combined_daily_prices as
             (
                 select DISTINCT ON (
                     t.symbol,
                     t.date
                     ) t.*
                 from (
                         (
                             select code as symbol,
                                    date,
                                    open,
                                    high,
                                    low,
                                    close,
                                    adjusted_close,
                                    volume,
                                    0 as priority
                             from {{ ref('historical_prices') }}
                             where date >= now() - interval '1 year' - interval '1 week'
                         )
                         union all
                         (
                             with filtered_base_tickers as
                                  (
                                      select symbol, exchange_canonical from {{ ref('base_tickers') }} where exchange_canonical is not null
                                  ),
                                  time_series_1d as
                                       (
                                           SELECT distinct exchange_canonical, date
                                           FROM {{ ref('historical_prices') }}
                                                    join filtered_base_tickers on filtered_base_tickers.symbol = historical_prices.code
                                           where date >= now() - interval '1 year' - interval '1 week'
                                       )
                             select symbol,
                                    time_series_1d.date,
                                    null::double precision as open,
                                    null::double precision as high,
                                    null::double precision as low,
                                    null::double precision as close,
                                    null::double precision as volume,
                                    null::double precision as adjusted_close,
                                    1                      as priority
                             from filtered_base_tickers
                                      join time_series_1d using (exchange_canonical)
                         )
                         union all
                         (
                             with filtered_base_tickers as
                                       (
                                          select symbol, country_name
                                           from {{ ref('base_tickers') }}
                                           where exchange_canonical is null
                                             and (country_name in ('USA') or country_name is null)
                                       ),
                                  time_series_1d as
                                       (
                                           SELECT distinct country_name, date
                                            FROM {{ ref('historical_prices') }}
                                                     join filtered_base_tickers on filtered_base_tickers.symbol = historical_prices.code
                                               where date >= now() - interval '1 year' - interval '1 week'
                                       )
                             select symbol,
                                    time_series_1d.date,
                                    null::double precision as open,
                                    null::double precision as high,
                                    null::double precision as low,
                                    null::double precision as close,
                                    null::double precision as volume,
                                    null::double precision as adjusted_close,
                                    1                      as priority
                             from filtered_base_tickers
                                      join time_series_1d using (country_name)
                          )
                     ) t
                 join {{ ref('base_tickers') }} using (symbol)
                 left join {{ ref('exchange_holidays') }}
                           on (exchange_holidays.exchange_name = base_tickers.exchange_canonical or
                               (base_tickers.exchange_canonical is null and exchange_holidays.country_name = base_tickers.country_name))
                               and exchange_holidays.date = t.date
                 where exchange_holidays.date is null
                 order by t.symbol, t.date, priority
             )
    select *
    from (
             select DISTINCT ON (
                 symbol,
                 date
                  ) (symbol || '_' || date || '_1d')::varchar as id,
                    symbol,
                    date::timestamp                           as time,
                    date::timestamp                           as datetime,
                    '1d'::varchar                                 as period,
                    coalesce(
                            open,
                            first_value(close)
                            OVER (partition by symbol, grp order by date)
                        )::double precision                       as open,
                    coalesce(
                            high,
                            first_value(close)
                            OVER (partition by symbol, grp order by date)
                        )::double precision                       as high,
                    coalesce(
                            low,
                            first_value(close)
                            OVER (partition by symbol, grp order by date)
                        )::double precision                       as low,
                    coalesce(
                            close,
                            first_value(close)
                            OVER (partition by symbol, grp order by date)
                        )::double precision                       as close,
                    coalesce(
                            adjusted_close,
                            first_value(adjusted_close)
                            OVER (partition by symbol, grp order by date)
                        )::double precision                       as adjusted_close,
                    coalesce(volume, 0)::double precision         as volume
             from (
                      select combined_daily_prices.*,
                             sum(case when close is not null then 1 end)
                             over (partition by combined_daily_prices.symbol order by date) as grp
                      from combined_daily_prices
{% if is_incremental() %}
                      left join max_date on max_date.symbol = combined_daily_prices.symbol and max_date.period = '1d'
                      where (max_date.time is null or combined_daily_prices.date >= max_date.time - interval '1 week')
{% endif %}
                  ) t
             order by symbol, date
         ) t2
    where t2.close is not null
)

union all

-- 1w
-- Execution Time: 132505.948 ms on test
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

-- 1m
-- Execution Time: 350440.336 ms
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