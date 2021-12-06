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
             from historical_prices_aggregated
             group by symbol, period
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
                 left join max_date on max_date.symbol = eod_intraday_prices.symbol and max_date.period = '15min'
                 where max_date.time is null or eod_intraday_prices.time >= max_date.time
                 {% endif %}
             )
    select DISTINCT ON (
        symbol,
        time_truncated
        ) (symbol || '_' || time_truncated || '_15min')::varchar                                                         as id,
          symbol                                                                                                         as symbol,
          time_truncated::timestamp                                                                                      as time, -- TODO remove
          time_truncated::timestamp                                                                                      as datetime,
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
       date::timestamp                         as datetime,
       '1d'::varchar                           as period,
       open::double precision,
       high::double precision,
       low::double precision,
       close::double precision,
       adjusted_close::double precision,
       volume::double precision
from {{ ref('historical_prices') }}
{% if is_incremental() %}
left join max_date on max_date.symbol = code and max_date.period = '1d'
where max_date.time is null or date > max_date.time
{% endif %}

union all

(
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
          OVER (partition by code, date_trunc('week', date)))::double precision                                             as volume
    from {{ ref('historical_prices') }}
    {% if is_incremental() %}
    left join max_date on max_date.symbol = code and max_date.period = '1w'
    where max_date.time is null or date >= max_date.time
    {% endif %}
    order by code, date_trunc('week', date), date
)

union all

(
    select DISTINCT ON (
        code,
        date_trunc('month', date)
        ) (code || '_' || date_trunc('month', date) || '_1m')::varchar                                                                                                    as id,
          code                                                                                                               as symbol,
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
          OVER (partition by code, date_trunc('month', date)))::double precision                                             as volume
    from {{ ref('historical_prices') }}
    {% if is_incremental() %}
    left join max_date on max_date.symbol = code and max_date.period = '1m'
    where max_date.time is null or date >= max_date.time
    {% endif %}
    order by code, date_trunc('month', date), date
)