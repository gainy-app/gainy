{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists {{ get_index_name(this, "symbol__period__datetime") }} (symbol, period, datetime)',
      'create index if not exists {{ get_index_name(this, "period__datetime") }} (period, datetime)',
    ],
  )
}}

-- dbt hint:
-- depends_on: {{ ref('base_tickers') }}

{% if is_incremental() and var('realtime') %}
(
    with period_settings as
             (
                 SELECT date_trunc('minute', dd) - interval '1 minute' as period_start,
                        date_trunc('minute', dd)                       as period_end
                 FROM generate_series(now()::timestamp - interval '30 minutes', now()::timestamp - interval '15 minutes',
                                      interval '1 minutes') dd
    -- uncomment when we have realtime prices
    --              FROM generate_series(now()::timestamp - interval '15 minutes', now()::timestamp, interval '1 minutes') dd
             ),
         expanded_intraday_prices as
             (
                 select distinct on (
                     symbol,
                     period_start
                     ) eod_intraday_prices.symbol,
                       time,
                       case
                           when eod_intraday_prices.time = period_start then eod_intraday_prices.open
                           else eod_intraday_prices.close end as open,
                       case
                           when eod_intraday_prices.time = period_start then eod_intraday_prices.high
                           else eod_intraday_prices.close end as high,
                       case
                           when eod_intraday_prices.time = period_start then eod_intraday_prices.low
                           else eod_intraday_prices.close end as low,
                       eod_intraday_prices.close              as close,
                       case
                           when eod_intraday_prices.time = period_start then eod_intraday_prices.volume
                           else 0.0 end                       as volume,
                       period_settings.period_start
                 from {{ source('eod', 'eod_intraday_prices') }}
                          join period_settings
                               on eod_intraday_prices.time >= now() - interval '30 minutes'
                                   and eod_intraday_prices.time < period_settings.period_end
                 order by symbol, period_start, time desc
             ),
         rolling_data as
             (
                 select DISTINCT ON (
                     expanded_intraday_prices.symbol,
                     period_start
                     ) expanded_intraday_prices.symbol                                                                                                                           as symbol,
                       period_start,
                       first_value(open::double precision)
                       OVER (partition by expanded_intraday_prices.symbol, period_start order by expanded_intraday_prices.time rows between current row and unbounded following) as open,
                       max(high::double precision)
                       OVER (partition by expanded_intraday_prices.symbol, period_start rows between current row and unbounded following)                                        as high,
                       min(low::double precision)
                       OVER (partition by expanded_intraday_prices.symbol, period_start rows between current row and unbounded following)                                        as low,
                       last_value(close::double precision)
                       OVER (partition by expanded_intraday_prices.symbol, period_start order by expanded_intraday_prices.time rows between current row and unbounded following) as close,
                       (sum(volume::numeric)
                        OVER (partition by expanded_intraday_prices.symbol, period_start rows between current row and unbounded following))::double precision                    as volume
                 from expanded_intraday_prices
                 order by symbol, period_start, time
             )
    select (symbol || '_' ||
            period_start || '_1min')::varchar as id,
           symbol,
           period_start                       as time,
           period_start                       as datetime,
           '1min'::varchar                    as period,
           open                               as open,
           high                               as high,
           low                                as low,
           close                              as close,
           close                              as adjusted_close,
           coalesce(volume, 0)                as volume
    from rolling_data
)

union all

(
    with period_settings as
             (
                 SELECT date_trunc('minute', dd) -
                        interval '1 minute' *
                        (mod(extract(minutes from dd)::int, 15) + 15) as period_start,
                        date_trunc('minute', dd) -
                        interval '1 minute' *
                        (mod(extract(minutes from dd)::int, 15)) as period_end
                 FROM generate_series(now()::timestamp - interval '15 minutes', now()::timestamp, interval '15 minutes') dd
    -- uncomment when we have realtime prices
    --              FROM generate_series(now()::timestamp - interval '15 minutes', now()::timestamp + interval '15 minutes', interval '15 minutes') dd
             ),
         expanded_intraday_prices as
             (
                 select eod_intraday_prices.*,
                        period_settings.period_start
                 from {{ source('eod', 'eod_intraday_prices') }}
                          join period_settings
                               on eod_intraday_prices.time >= period_settings.period_start
                                   and eod_intraday_prices.time < period_settings.period_end
             ),
         new_data as
             (
                 select DISTINCT ON (
                     expanded_intraday_prices.symbol,
                     period_start
                     ) expanded_intraday_prices.symbol                                                                                                                           as symbol,
                       period_start,
                       first_value(open::double precision)
                       OVER (partition by expanded_intraday_prices.symbol, period_start order by expanded_intraday_prices.time rows between current row and unbounded following) as open,
                       max(high::double precision)
                       OVER (partition by expanded_intraday_prices.symbol, period_start rows between current row and unbounded following)                                        as high,
                       min(low::double precision)
                       OVER (partition by expanded_intraday_prices.symbol, period_start rows between current row and unbounded following)                                        as low,
                       last_value(close::double precision)
                       OVER (partition by expanded_intraday_prices.symbol, period_start order by expanded_intraday_prices.time rows between current row and unbounded following) as close,
                       (sum(volume::numeric)
                        OVER (partition by expanded_intraday_prices.symbol, period_start rows between current row and unbounded following))::double precision                    as volume
                 from expanded_intraday_prices
                 order by symbol, period_start, time
             ),
         latest_old_data as
             (
                 select distinct on (symbol) id,
                                             datetime as period_start,
                                             period,
                                             close,
                                             adjusted_close,
                                             symbol
                 from {{ this }}
                 where period = '15min'
                   and datetime > now() - interval '4 days'
                 order by symbol, datetime desc
             )
    select (base_tickers.symbol || '_' ||
            period_settings.period_start || '_15min')::varchar      as id,
           base_tickers.symbol,
           period_settings.period_start                             as time,
           period_settings.period_start                             as datetime,
           '15min'::varchar                                         as period,
           coalesce(new_data.open, latest_old_data.close)           as open,
           coalesce(new_data.high, latest_old_data.close)           as high,
           coalesce(new_data.low, latest_old_data.close)            as low,
           coalesce(new_data.close, latest_old_data.close)          as close,
           coalesce(new_data.close, latest_old_data.adjusted_close) as adjusted_close,
           coalesce(new_data.volume, 0)                             as volume
    from {{ ref('base_tickers') }}
             join period_settings on true
             left join new_data
                       on new_data.symbol = base_tickers.symbol
                              and new_data.period_start = period_settings.period_start
             left join latest_old_data on latest_old_data.symbol = base_tickers.symbol
    where new_data.symbol is not null
       or latest_old_data.symbol is not null
)

-- end realtime
{% else %}


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
         period_settings as
             (
                 SELECT date_trunc('minute', dd) - interval '1 minute' as period_start,
                        date_trunc('minute', dd)                       as period_end
                 FROM latest_open_trading_session
                          join generate_series(latest_open_trading_session.open_at,
                                               least(latest_open_trading_session.close_at,
                                                   now()::timestamp - interval '15 minutes'),
                                               interval '1 minutes') dd on true
             ),
         expanded_intraday_prices as
             (
                 select distinct on (
                     symbol,
                     period_start
                     ) eod_intraday_prices.symbol,
                       time,
                       case
                           when eod_intraday_prices.time = period_start then eod_intraday_prices.open
                           else eod_intraday_prices.close end as open,
                       case
                           when eod_intraday_prices.time = period_start then eod_intraday_prices.high
                           else eod_intraday_prices.close end as high,
                       case
                           when eod_intraday_prices.time = period_start then eod_intraday_prices.low
                           else eod_intraday_prices.close end as low,
                       eod_intraday_prices.close              as close,
                       case
                           when eod_intraday_prices.time = period_start then eod_intraday_prices.volume
                           else 0.0 end                       as volume,
                       period_settings.period_start
                 from {{ source('eod', 'eod_intraday_prices') }}
                          join period_settings
                               on eod_intraday_prices.time >= period_settings.period_start - interval '1 hour'
                                   and eod_intraday_prices.time < period_settings.period_end
                 order by symbol, period_start, time desc
             ),
         rolling_data as
             (
                 select DISTINCT ON (
                     expanded_intraday_prices.symbol,
                     period_start
                     ) expanded_intraday_prices.symbol                                                                                                                           as symbol,
                       period_start,
                       first_value(open::double precision)
                       OVER (partition by expanded_intraday_prices.symbol, period_start order by expanded_intraday_prices.time rows between current row and unbounded following) as open,
                       max(high::double precision)
                       OVER (partition by expanded_intraday_prices.symbol, period_start rows between current row and unbounded following)                                        as high,
                       min(low::double precision)
                       OVER (partition by expanded_intraday_prices.symbol, period_start rows between current row and unbounded following)                                        as low,
                       last_value(close::double precision)
                       OVER (partition by expanded_intraday_prices.symbol, period_start order by expanded_intraday_prices.time rows between current row and unbounded following) as close,
                       (sum(volume::numeric)
                        OVER (partition by expanded_intraday_prices.symbol, period_start rows between current row and unbounded following))::double precision                    as volume
                 from expanded_intraday_prices
                 order by symbol, period_start, time
             )
    select (symbol || '_' ||
            period_start || '_1min')::varchar as id,
           symbol,
           period_start                       as time,
           period_start                       as datetime,
           '1min'::varchar                    as period,
           open                               as open,
           high                               as high,
           low                                as low,
           close                              as close,
           close                              as adjusted_close,
           coalesce(volume, 0)                as volume
    from rolling_data
)

union all

(
    with time_series_15min as
             (
                 SELECT date_trunc('minute', dd) -
                        interval '1 minute' *
                        (mod(extract(minutes from dd)::int, 15) + 15) as datetime
                 FROM generate_series(now()::timestamp - interval '1 week', now()::timestamp, interval '15 minutes') dd
             ),
         expanded_intraday_prices as
             (
                 select eod_intraday_prices.*,
                        date_trunc('minute', eod_intraday_prices.time) -
                        interval '1 minute' *
                        mod(extract(minutes from eod_intraday_prices.time)::int, 15) as time_truncated
                 from {{ source('eod', 'eod_intraday_prices') }}
                 where eod_intraday_prices.time >= now() - interval '1 week'
             ),
         combined_intraday_prices as
             (
                 select DISTINCT ON (
                     expanded_intraday_prices.symbol,
                     time_truncated
                     ) (expanded_intraday_prices.symbol || '_' || time_truncated || '_15min')::varchar                                                                             as id,
                       expanded_intraday_prices.symbol                                                                                                                             as symbol,
                       time_truncated::timestamp                                                                                                                                   as time, -- TODO remove
                       time_truncated::timestamp                                                                                                                                   as datetime,
                       '15min'::varchar                                                                                                                                            as period,
                       first_value(open::double precision)
                       OVER (partition by expanded_intraday_prices.symbol, time_truncated order by expanded_intraday_prices.time rows between current row and unbounded following) as open,
                       max(high::double precision)
                       OVER (partition by expanded_intraday_prices.symbol, time_truncated rows between current row and unbounded following)                                        as high,
                       min(low::double precision)
                       OVER (partition by expanded_intraday_prices.symbol, time_truncated rows between current row and unbounded following)                                        as low,
                       last_value(close::double precision)
                       OVER (partition by expanded_intraday_prices.symbol, time_truncated order by expanded_intraday_prices.time rows between current row and unbounded following) as close,
                       (sum(volume::numeric)
                        OVER (partition by expanded_intraday_prices.symbol, time_truncated rows between current row and unbounded following))::double precision                    as volume
                 from expanded_intraday_prices
                 order by symbol, time_truncated, time
             )
    select distinct on (
                    combined_intraday_prices.symbol, time_series_15min.datetime
        ) (combined_intraday_prices.symbol || '_' || time_series_15min.datetime || '_15min')::varchar as id,
          combined_intraday_prices.symbol,
          time_series_15min.datetime                                                                  as time,
          time_series_15min.datetime                                                                  as datetime,
          combined_intraday_prices.period,
          case
              when combined_intraday_prices.datetime = time_series_15min.datetime then combined_intraday_prices.open
              else combined_intraday_prices.close end                                                 as open,
          case
              when combined_intraday_prices.datetime = time_series_15min.datetime then combined_intraday_prices.high
              else combined_intraday_prices.close end                                                 as high,
          case
              when combined_intraday_prices.datetime = time_series_15min.datetime then combined_intraday_prices.low
              else combined_intraday_prices.close end                                                 as low,
          combined_intraday_prices.close                                                              as close,
          combined_intraday_prices.close                                                              as adjusted_close,
          case
              when combined_intraday_prices.datetime = time_series_15min.datetime then combined_intraday_prices.volume
              else 0.0 end                                                                            as volume
    from combined_intraday_prices
             join time_series_15min
                  on time_series_15min.datetime >= combined_intraday_prices.datetime or
                     combined_intraday_prices.datetime is null
    order by combined_intraday_prices.symbol, time_series_15min.datetime, combined_intraday_prices.datetime desc
)

union all

(
    with time_series_1d as
        (
            SELECT distinct date::timestamp as datetime
            FROM historical_prices
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
                 FROM historical_prices
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
                 FROM historical_prices
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