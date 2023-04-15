{% set minutes = 15 %}

{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('symbol, datetime'),
      index('id', true),
      'create index if not exists "hpa_15min_datetime__symbol" ON {{ this }} (datetime, symbol)',
    ]
  )
}}


-- Execution Time: 228534.729 ms
with
{% if is_incremental() %}
     max_date as materialized
         (
             select symbol, max(datetime) as datetime
             from {{ this }}
             group by symbol
         ),
     latest_known_prices as
         (
             select symbol, adjusted_close, updated_at
             from max_date
             join {{ this }} using (symbol, datetime)
         ),
{% endif %}
     time_series as
         (
             select symbol,
                    week_trading_sessions_static.date,
                    date_trunc('minute', dd) -
                    interval '1 minute' *
                    mod(extract(minutes from dd)::int, {{ minutes }}) as time_truncated
             from {{ ref('week_trading_sessions_static') }}
                      join generate_series(open_at,
                                           least(
                                                   now(),
                                                   close_at - interval '1 second',
                                                   (select max(time) - interval '15 minutes' from {{ ref('historical_intraday_prices') }} where source = 'polygon')
                                               ),
                                           interval '{{ minutes }} minutes') dd on true
{% if is_incremental() and var('realtime') %}
                      join max_date using (symbol)
{% endif %}

             where week_trading_sessions_static.index >= 0

{% if is_incremental() and var('realtime') %}
               and dd > max_date.datetime - interval '30 minutes'
{% endif %}
         ),
     expanded_intraday_prices as
         (
             select historical_intraday_prices.*,
                    historical_intraday_prices.time_{{ minutes }}min as time_truncated
             from {{ ref('historical_intraday_prices') }}
                      join {{ ref('week_trading_sessions_static') }} using (symbol, date)
{% if is_incremental() and var('realtime') %}
                      join max_date using (symbol)
             where historical_intraday_prices.time_{{ minutes }}min > max_date.datetime - interval '30 minutes'
               and week_trading_sessions_static.index >= 0
{% endif %}
         ),
     combined_intraday_prices as
         (
             select DISTINCT ON (
                 symbol,
                 time_truncated
                 ) symbol,
                   date,
                   time_truncated::timestamp                                                                                                as datetime,
                   first_value(open)
                   OVER (partition by symbol, time_truncated order by time, priority desc rows between current row and unbounded following) as open,
                   max(high)
                   OVER (partition by symbol, time_truncated order by time, priority desc rows between current row and unbounded following)                              as high,
                   min(low)
                   OVER (partition by symbol, time_truncated order by time, priority desc rows between current row and unbounded following)                              as low,
                   last_value(close)
                   OVER (partition by symbol, time_truncated order by time, priority desc rows between current row and unbounded following) as close,
                   last_value(adjusted_close)
                   OVER (partition by symbol, time_truncated order by time, priority desc rows between current row and unbounded following) as adjusted_close,
                   (sum(volume)
                    OVER (partition by symbol, time_truncated order by time, priority desc rows between current row and unbounded following))::double precision          as volume,
                   max(updated_at)
                   OVER (partition by symbol, time_truncated order by time, priority desc rows between current row and unbounded following)                              as updated_at
             from (
                      select symbol,
                             date,
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
                             date,
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
                             date,
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
             where t.time_truncated < now() - interval '15 minutes' -- close_datetime must be less than 15 minutes ago
             order by symbol, time_truncated, time, priority
         )
select t3.id,
       t3.symbol,
       t3.date,
       t3.datetime,
       t3.open,
       t3.high,
       t3.low,
       t3.close,
       t3.adjusted_close,
       t3.relative_gain,
       t3.volume,
       coalesce(t3.updated_at, now())::timestamp as updated_at
from (
         select t2.symbol || '_' || t2.datetime                                            as id,
                t2.symbol,
                t2.date,
                t2.datetime,
                case
                    when lag(t2.adjusted_close) over wnd > 0
                        then coalesce(t2.adjusted_close::numeric / (lag(t2.adjusted_close) over wnd)::numeric - 1, 0)
                    end                                                                    as relative_gain,
{% if is_incremental() %}
                coalesce(t2.open,
                         old_data.open,
                         latest_known_prices.adjusted_close,
                         historical_prices_aggregated_1d.adjusted_close)::double precision as open,
                coalesce(t2.high,
                         old_data.high,
                         latest_known_prices.adjusted_close,
                         historical_prices_aggregated_1d.adjusted_close)::double precision as high,
                coalesce(t2.low,
                         old_data.low,
                         latest_known_prices.adjusted_close,
                         historical_prices_aggregated_1d.adjusted_close)::double precision as low,
                coalesce(t2.close,
                         old_data.close,
                         latest_known_prices.adjusted_close,
                         historical_prices_aggregated_1d.adjusted_close)::double precision as close,
                coalesce(t2.adjusted_close,
                         old_data.adjusted_close,
                         latest_known_prices.adjusted_close,
                         historical_prices_aggregated_1d.adjusted_close)::double precision as adjusted_close,
                coalesce(t2.volume, old_data.volume, 0)                                    as volume,
                t2.updated_at,
                old_data.adjusted_close                                                    as old_data_adjusted_close,
                old_data.relative_gain                                                     as old_data_relative_gain
{% else %}
                coalesce(t2.open,
                         historical_prices_aggregated_1d.adjusted_close)::double precision as open,
                coalesce(t2.high,
                         historical_prices_aggregated_1d.adjusted_close)::double precision as high,
                coalesce(t2.low,
                         historical_prices_aggregated_1d.adjusted_close)::double precision as low,
                coalesce(t2.close,
                         historical_prices_aggregated_1d.adjusted_close)::double precision as close,
                coalesce(t2.adjusted_close,
                         historical_prices_aggregated_1d.adjusted_close)::double precision as adjusted_close,
                coalesce(t2.volume, 0)                                                     as volume,
                t2.updated_at
{% endif %}
         from (
                   select symbol,
                          date,
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
                          updated_at
                   from (
                            select *,
                                   coalesce(sum(case when adjusted_close is not null then 1 end)
                                            over (partition by symbol order by datetime), 0) as grp
                            from combined_intraday_prices
                        ) t
              ) t2
                  left join {{ ref('week_trading_sessions_static') }} using (symbol, date)
                  left join {{ ref('historical_prices_aggregated_1d') }}
                            on historical_prices_aggregated_1d.symbol = t2.symbol
                                and historical_prices_aggregated_1d.datetime = week_trading_sessions_static.prev_date
{% if is_incremental() %}
                  left join latest_known_prices
                            on latest_known_prices.symbol = t2.symbol
                  left join {{ this }} old_data
                            on old_data.symbol = t2.symbol
                                and old_data.datetime = t2.datetime
{% endif %}
         where week_trading_sessions_static.index >= 0
                  window wnd as (partition by t2.symbol order by t2.datetime rows between 1 preceding and current row)
    ) t3
where adjusted_close is not null
{% if is_incremental() %}
  and (old_data_adjusted_close is null -- no old data
   or (t3.relative_gain is not null and old_data_relative_gain is null)
   or abs(t3.relative_gain - old_data_relative_gain) > {{ var('price_precision') }}
   or abs(t3.adjusted_close - old_data_adjusted_close) > {{ var('price_precision') }})
{% endif %}
