{{
  config(
    materialized = "incremental",
    unique_key = "id",
    incremental_strategy = 'insert_overwrite',
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists {{ get_index_name(this, "symbol__period__time") }} (symbol, period, time)',
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
    with time_series_15min as
             (
                 SELECT date_trunc('minute', dd) -
                        interval '1 minute' *
                        (mod(extract(minutes from dd)::int, 15) + 15) as datetime
                 FROM generate_series(now()::timestamp without time zone - interval '1 day', now()::timestamp without time zone, interval '15 minutes') dd
             ),
         expanded_intraday_prices as
             (
                 select eod_intraday_prices.*,
                        date_trunc('minute', eod_intraday_prices.time) -
                        interval '1 minute' *
                        mod(extract(minutes from eod_intraday_prices.time)::int, 15) as time_truncated
                 from {{ ref('tickers') }}
                          left join {{ source('eod', 'eod_intraday_prices') }} on eod_intraday_prices.symbol = tickers.symbol
    -- {% if is_incremental() %}
    --                       left join max_date
    --                                 on max_date.symbol = eod_intraday_prices.symbol and max_date.period = '15min'
    --              where max_date.time is null
    --                 or eod_intraday_prices.time >= max_date.time
    -- {% endif %}
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
                       last_value(close::double precision)
                       OVER (partition by expanded_intraday_prices.symbol, time_truncated order by expanded_intraday_prices.time rows between current row and unbounded following) as adjusted_close,
                       (sum(volume::numeric)
                        OVER (partition by expanded_intraday_prices.symbol, time_truncated rows between current row and unbounded following))::double precision                    as volume
                 from expanded_intraday_prices
    -- {% if is_incremental() %}
    --                       left join max_date
    --                                 on max_date.symbol = expanded_intraday_prices.symbol and max_date.period = '15min'
    -- {% endif %}
                 where time_truncated < now() - interval '15 minutes'
    -- {% if is_incremental() %}
    --                and (max_date.time is null or expanded_intraday_prices.time_truncated > max_date.time)
    -- {% endif %}
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
          case
              when combined_intraday_prices.datetime = time_series_15min.datetime then combined_intraday_prices.close
              else combined_intraday_prices.close end                                                 as close,
          case
              when combined_intraday_prices.datetime = time_series_15min.datetime
                  then combined_intraday_prices.adjusted_close
              else combined_intraday_prices.adjusted_close end                                        as adjusted_close,
          case
              when combined_intraday_prices.datetime = time_series_15min.datetime then combined_intraday_prices.volume
              else 0.0 end                                                                            as volume
    from {{ ref('tickers') }}
             join combined_intraday_prices on combined_intraday_prices.symbol = tickers.symbol
             join time_series_15min
                  on time_series_15min.datetime >= combined_intraday_prices.datetime or
                     combined_intraday_prices.datetime is null
--     order by combined_intraday_prices.symbol || '_' || time_series_15min.datetime || '_15min', combined_intraday_prices.datetime desc
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
        )(code || '_' || date || '_1d')::varchar as id,
         code                                    as symbol,
         time_series_1d.datetime                 as time,
         time_series_1d.datetime                 as datetime,
         '1d'::varchar                           as period,
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
        ) id,
          symbol,
          time_series_1w.datetime                       as time,
          time_series_1w.datetime                       as datetime,
          period,
          case
              when historical_prices.datetime = time_series_1w.datetime then historical_prices.open
              else historical_prices.close end          as open,
          case
              when historical_prices.datetime = time_series_1w.datetime then historical_prices.high
              else historical_prices.close end          as high,
          case
              when historical_prices.datetime = time_series_1w.datetime then historical_prices.low
              else historical_prices.close end          as low,
          case
              when historical_prices.datetime = time_series_1w.datetime then historical_prices.close
              else historical_prices.close end          as close,
          case
              when historical_prices.datetime = time_series_1w.datetime
                  then historical_prices.adjusted_close
              else historical_prices.adjusted_close end as adjusted_close,
          case
              when historical_prices.datetime = time_series_1w.datetime then historical_prices.volume
              else 0.0 end                              as volume
    from (
             select DISTINCT ON (
                 code,
                 date_trunc('week', date)
                 ) (code || '_' || date_trunc('week', date) || '_1w')::varchar                                                       as id,
                   code                                                                                                              as symbol,
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
        ) id,
          symbol,
          time_series_1m.datetime                       as time,
          time_series_1m.datetime                       as datetime,
          period,
          case
              when historical_prices.datetime = time_series_1m.datetime then historical_prices.open
              else historical_prices.close end          as open,
          case
              when historical_prices.datetime = time_series_1m.datetime then historical_prices.high
              else historical_prices.close end          as high,
          case
              when historical_prices.datetime = time_series_1m.datetime then historical_prices.low
              else historical_prices.close end          as low,
          case
              when historical_prices.datetime = time_series_1m.datetime then historical_prices.close
              else historical_prices.close end          as close,
          case
              when historical_prices.datetime = time_series_1m.datetime
                  then historical_prices.adjusted_close
              else historical_prices.adjusted_close end as adjusted_close,
          case
              when historical_prices.datetime = time_series_1m.datetime then historical_prices.volume
              else 0.0 end                              as volume
    from (
             select DISTINCT ON (
                 code,
                 date_trunc('month', date)
                 ) (code || '_' || date_trunc('month', date) || '_1m')::varchar                                                       as id,
                   code                                                                                                              as symbol,
                   date_trunc('month', date)::timestamp                                                                               as time,
                   date_trunc('month', date)::timestamp                                                                               as datetime,
                   '1m'::varchar                                                                                                     as period,
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
