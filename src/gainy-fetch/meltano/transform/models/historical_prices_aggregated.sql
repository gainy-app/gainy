{{
  config(
    materialized = "incremental",
    unique_key = "id",
    incremental_strategy = 'insert_overwrite',
    post_hook=[
      index(this, 'id', true),
      index(this, 'symbol'),
      index(this, 'time'),
      index(this, 'period'),
      'create unique index if not exists {{ get_index_name(this, "symbol__time__period") }} (symbol, time, period)',
    ]
  )
}}


{% if is_incremental() %}
with max_date_15min as
         (
             select symbol,
                    max(time) as time
             from {{ this }}
             where period = '15min'
             group by symbol
         ),
     max_date_1d as
         (
             select symbol,
                    max(time) as time
             from {{ this }}
             where period = '1d'
             group by symbol
         ),
     max_date_1w as
         (
             select symbol,
                    max(time) as time
             from {{ this }}
             where period = '1w'
             group by symbol
         ),
     max_date_1m as
         (
             select symbol,
                    max(time) as time
             from {{ this }}
             where period = '1m'
             group by symbol
         )
{% endif %}
(
    with expanded_intraday_prices as
             (
                 select eod_intraday_prices.*,
                        date_trunc('minute', eod_intraday_prices.time) -
                        interval '1 minute' * mod(extract(minutes from eod_intraday_prices.time)::int, 15) as time_truncated
                 from {{ source('eod', 'eod_intraday_prices') }}
                 {% if is_incremental() %}
                 left join max_date_15min on max_date_15min.symbol = eod_intraday_prices.symbol
                 where max_date_15min.time is null or eod_intraday_prices.time >= max_date_15min.time
                 {% endif %}
             )
    select DISTINCT ON (
        symbol,
        time_truncated
        ) (symbol || '_' || time_truncated || '_15min')::varchar                                                         as id,
          symbol                                                                                                         as symbol,
          time_truncated::timestamp                                                                                      as time,
          '15min'::varchar                                                                                               as period,
          first_value(open::double precision)
          OVER (partition by symbol, time_truncated order by time rows between current row and unbounded following)      as open,
          max(high::double precision)
          OVER (partition by symbol, time_truncated rows between current row and unbounded following)                    as high,
          min(low::double precision)
          OVER (partition by symbol, time_truncated rows between current row and unbounded following)                    as low,
          last_value(close::double precision)
          OVER (partition by symbol, time_truncated order by time rows between current row and unbounded following)      as close,
          last_value(close::double precision)
          OVER (partition by symbol, time_truncated order by time rows between current row and unbounded following)      as adjusted_close,
          (sum(volume::numeric)
          OVER (partition by symbol, time_truncated rows between current row and unbounded following))::double precision as volume
    from expanded_intraday_prices
    order by symbol, time_truncated, time
)

union all

select (code || '_' || date || '_1d')::varchar as id,
       code                                    as symbol,
       date::timestamp                         as time,
       '1d'::varchar                           as period,
       open::double precision,
       high::double precision,
       low::double precision,
       close::double precision,
       adjusted_close::double precision,
       volume::double precision
from {{ ref('historical_prices') }}
{% if is_incremental() %}
left join max_date_1d on max_date_1d.symbol = code
where max_date_1d.time is null or date > max_date_1d.time
{% endif %}
union all

(
    select DISTINCT ON (
        code,
        date_trunc('week', date)
        ) (code || '_' || date_trunc('week', date) || '_1w')::varchar                                                                                                   as id,
          code                                                                                                                                      as symbol,
          date_trunc('week', date)::timestamp                                                                                                       as time,
          '1w'::varchar                                                                                                                             as period,
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
          OVER (partition by code, date_trunc('week', date)))::double precision                                             as volume
    from {{ ref('historical_prices') }}
    {% if is_incremental() %}
    left join max_date_1w on max_date_1w.symbol = code
    where max_date_1w.time is null or date > max_date_1w.time
    {% endif %}
    order by code, date_trunc('week', date), date
)

union all

(
    select DISTINCT ON (
        code,
        date_trunc('month', date)
        ) (code || '_' || date_trunc('month', date) || '_1m')::varchar                                                                                                    as id,
          code                                                                                                                                       as symbol,
          date_trunc('month', date)::timestamp                                                                                                       as time,
          '1m'::varchar                                                                                                                              as period,
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
          OVER (partition by code, date_trunc('month', date)))::double precision                                             as volume
    from {{ ref('historical_prices') }}
    {% if is_incremental() %}
    left join max_date_1m on max_date_1m.symbol = code
    where max_date_1m.time is null or date > max_date_1m.time
    {% endif %}
    order by code, date_trunc('month', date), date
)