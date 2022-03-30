{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists "symbol__period__datetime" ON {{ this }} (symbol, period, datetime)',
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

(
    with combined_daily_prices as
             (
                 (
                     select code as symbol,
                            date,
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
                                  FROM {{ ref('historical_prices') }}
                                  where date >= now() - interval '1 year' - interval '1 week'
                              )
                     select code                   as symbol,
                            time_series_1d.date,
                            null::double precision as open,
                            null::double precision as high,
                            null::double precision as low,
                            null::double precision as close,
                            null::double precision as volume,
                            null::double precision as adjusted_close
                     from (select distinct code from {{ ref('historical_prices') }}) t1
                              join time_series_1d on true
                     where not exists(select 1
                                      from {{ ref('historical_prices') }}
                                      where historical_prices.code = t1.code
                                        and historical_prices.date = time_series_1d.date)
                 )
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
                            close,
                            first_value(close)
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
