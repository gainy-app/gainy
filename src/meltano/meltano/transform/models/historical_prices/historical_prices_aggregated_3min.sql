{% set minutes = 3 %}

{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('symbol, datetime'),
      index(this, 'id', true),
      'create index if not exists "datetime__symbol" ON {{ this }} (datetime, symbol)',
    ]
  )
}}


-- Execution Time: 100765.485 ms
with
{% if is_incremental() %}
     max_date as  materialized
         (
             select symbol, max(datetime) as datetime
             from {{ this }}
             group by symbol
         ),
     latest_known_prices as
         (
             select symbol, adjusted_close
             from max_date
             join {{ this }} using (symbol, datetime)
         ),
{% endif %}
     latest_open_trading_session as
         (
             select *
             from {{ ref('week_trading_sessions') }}
             where index = 0
         ),
     time_series as
         (
             select symbol,
                    date_trunc('minute', dd) -
                    interval '1 minute' *
                    mod(extract(minutes from dd)::int, {{ minutes }}) as time_truncated
             from {{ ref('week_trading_sessions') }}
                      join generate_series(open_at, least(now(), close_at - interval '1 second'), interval '{{ minutes }} minutes') dd on true
{% if is_incremental() and var('realtime') %}
                      join max_date using (symbol)
{% endif %}
             where index = 0
{% if is_incremental() and var('realtime') %}
               and dd > max_date.datetime - interval '30 minutes'
{% endif %}
         ),
     expanded_intraday_prices as
         (
             select historical_intraday_prices.*,
                    historical_intraday_prices.time_{{ minutes }}min as time_truncated
             from {{ ref('historical_intraday_prices') }}
                      join latest_open_trading_session using (symbol)
{% if is_incremental() and var('realtime') %}
                      join max_date using (symbol)
{% endif %}
             where historical_intraday_prices.time_{{ minutes }}min >= latest_open_trading_session.open_at - interval '1 hour' and historical_intraday_prices.time_{{ minutes }}min < latest_open_trading_session.close_at
{% if is_incremental() and var('realtime') %}
               and historical_intraday_prices.time_{{ minutes }}min > max_date.datetime - interval '30 minutes'
{% endif %}
         ),
     combined_intraday_prices as
         (
             select DISTINCT ON (
                 symbol,
                 time_truncated
                 ) symbol                                                                                                                   as symbol,
                   time_truncated::timestamp                                                                                                as datetime,
                   first_value(open)
                   OVER (partition by symbol, time_truncated order by time, priority desc rows between current row and unbounded following) as open,
                   max(high)
                   OVER (partition by symbol, time_truncated rows between current row and unbounded following)                              as high,
                   min(low)
                   OVER (partition by symbol, time_truncated rows between current row and unbounded following)                              as low,
                   last_value(close)
                   OVER (partition by symbol, time_truncated order by time, priority desc rows between current row and unbounded following) as close,
                   last_value(adjusted_close)
                   OVER (partition by symbol, time_truncated order by time, priority desc rows between current row and unbounded following) as adjusted_close,
                   (sum(volume)
                    OVER (partition by symbol, time_truncated rows between current row and unbounded following))::double precision          as volume,
                   min(updated_at)
                   OVER (partition by symbol, time_truncated rows between current row and unbounded following)                              as updated_at
             from (
                      select symbol,
                             time,
                             open,
                             high,
                             low,
                             close,
                             adjusted_close,
                             volume,
                             time_truncated,
                             updated_at,
                             0 as priority
                      from expanded_intraday_prices
                      union all
                      select symbol,
                             time_truncated as time,
                             null      as open,
                             null      as high,
                             null      as low,
                             null      as close,
                             null      as adjusted_close,
                             null      as volume,
                             time_truncated,
                             null      as updated_at,
                             1         as priority
                      from {{ ref('base_tickers') }}
                               join time_series using (symbol)
                      union all
                      select contract_name,
                             time_truncated as time,
                             null      as open,
                             null      as high,
                             null      as low,
                             null      as close,
                             null      as adjusted_close,
                             null      as volume,
                             time_truncated,
                             null      as updated_at,
                             1         as priority
                      from {{ ref('ticker_options_monitored') }}
                               join time_series using (symbol)
                  ) t
             order by symbol, time_truncated, time, priority
         )
select t2.symbol || '_' || t2.datetime                               as id,
       t2.symbol,
       t2.datetime,
{% if is_incremental() %}
       coalesce(t2.open,
                old_data.open,
                latest_known_prices.adjusted_close,
                historical_prices_marked.price_0d)::double precision as open,
       coalesce(t2.high,
                old_data.high,
                latest_known_prices.adjusted_close,
                historical_prices_marked.price_0d)::double precision as high,
       coalesce(t2.low,
                old_data.low,
                latest_known_prices.adjusted_close,
                historical_prices_marked.price_0d)::double precision as low,
       coalesce(t2.close,
                old_data.close,
                latest_known_prices.adjusted_close,
                historical_prices_marked.price_0d)::double precision as close,
       coalesce(t2.adjusted_close,
                old_data.adjusted_close,
                latest_known_prices.adjusted_close,
                historical_prices_marked.price_0d)::double precision as adjusted_close,
       coalesce(t2.volume, old_data.volume, 0)                       as volume,
       coalesce(t2.updated_at, old_data.updated_at)                  as updated_at
{% else %}
       coalesce(t2.open,
                historical_prices_marked.price_0d)::double precision as open,
       coalesce(t2.high,
                historical_prices_marked.price_0d)::double precision as high,
       coalesce(t2.low,
                historical_prices_marked.price_0d)::double precision as low,
       coalesce(t2.close,
                historical_prices_marked.price_0d)::double precision as close,
       coalesce(t2.adjusted_close,
                historical_prices_marked.price_0d)::double precision as adjusted_close,
       coalesce(t2.volume, 0)                                        as volume,
       t2.updated_at
{% endif %}
from (
          select symbol,
                 datetime,
                 coalesce(
                         open,
                         first_value(close)
                         OVER (partition by symbol, grp order by datetime)) as open,
                 coalesce(
                         high,
                         first_value(close)
                         OVER (partition by symbol, grp order by datetime)) as high,
                 coalesce(
                         low,
                         first_value(close)
                         OVER (partition by symbol, grp order by datetime)) as low,
                 coalesce(
                         close,
                         first_value(close)
                         OVER (partition by symbol, grp order by datetime)) as close,
                 coalesce(
                         adjusted_close,
                         first_value(adjusted_close)
                         OVER (partition by symbol, grp order by datetime)) as adjusted_close,
                 volume,
                 coalesce(
                         updated_at,
                         first_value(updated_at)
                         OVER (partition by symbol, grp order by datetime)) as updated_at
          from (
                   select *,
                          coalesce(sum(case when adjusted_close is not null then 1 end)
                                   over (partition by symbol order by datetime), 0) as grp
                   from combined_intraday_prices
               ) t
     ) t2
         left join {{ ref('historical_prices_marked') }} using (symbol)
{% if is_incremental() %}
         left join latest_known_prices using (symbol)
         left join {{ this }} old_data using (symbol, datetime)
where (old_data.symbol is null -- no old data
   or (t2.updated_at is not null and old_data.updated_at is null) -- old data is null and new is not
   or t2.updated_at > old_data.updated_at) -- new data is newer than the old one
{% endif %}
