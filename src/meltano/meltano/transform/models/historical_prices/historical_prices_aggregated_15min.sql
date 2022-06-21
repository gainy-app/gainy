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


-- 15min
-- Execution Time: 226004.752 ms on test
with
{% if is_incremental() %}
     max_date as
         (
             select max(datetime) as datetime
             from {{ this }}
         ),
{% endif %}
     week_trading_sessions as
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
{% if is_incremental() and var('realtime') %}
                      join max_date on true
{% endif %}
             where time_truncated >= week_trading_sessions.open_at and time_truncated < week_trading_sessions.close_at
{% if is_incremental() and var('realtime') %}
               and time_truncated > max_date.datetime - interval '20 minutes'
{% endif %}
             union all
             SELECT 'crypto' as type,
                    date_trunc('minute', dd) -
                    interval '1 minute' *
                    mod(extract(minutes from dd)::int, 15) as time_truncated
             FROM generate_series(now()::timestamp - interval '1 week', now()::timestamp, interval '15 minutes') dd
{% if is_incremental() and var('realtime') %}
                      join max_date on true
             where dd > max_date.datetime - interval '20 minutes'
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
{% if is_incremental() and var('realtime') %}
                      join max_date on true
{% endif %}
             where (eod_intraday_prices.time >= week_trading_sessions.open_at - interval '1 hour' and eod_intraday_prices.time < week_trading_sessions.close_at
                or (symbol like '%.CC' and time > now() - interval '1 week'))
{% if is_incremental() and var('realtime') %}
               and eod_intraday_prices.time > max_date.datetime - interval '20 minutes'
{% endif %}
         ),
     combined_intraday_prices as
         (
             select DISTINCT ON (
                 symbol,
                 time_truncated
                 ) symbol                                                                                                    as symbol,
                   time_truncated::timestamp                                                                                 as datetime,
                   first_value(open)
                   OVER (partition by symbol, time_truncated order by time rows between current row and unbounded following) as open,
                   max(high)
                   OVER (partition by symbol, time_truncated rows between current row and unbounded following)               as high,
                   min(low)
                   OVER (partition by symbol, time_truncated rows between current row and unbounded following)               as low,
                   last_value(close)
                   OVER (partition by symbol, time_truncated order by time rows between current row and unbounded following) as close,
                   sum(volume)
                   OVER (partition by symbol, time_truncated rows between current row and unbounded following)               as volume
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
                      union all
                      select contract_name,
                             time_truncated as time,
                             null           as open,
                             null           as high,
                             null           as low,
                             null           as close,
                             null           as volume,
                             time_truncated,
                             1              as priority
                      from {{ ref('ticker_options_monitored') }}
                               join time_series_15min on time_series_15min.type is null
                  ) t
             order by symbol, time_truncated, time, priority
         )
select id,
       symbol,
       datetime,
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
           ) as adjusted_close,
       volume
from (
         select symbol || '_' || datetime                    as id,
                symbol,
                datetime,
                coalesce(
                        open,
                        first_value(close)
                        OVER (partition by symbol, grp order by datetime),
                        historical_prices_marked.price_1w
                    )                                        as open,
                coalesce(
                        high,
                        first_value(close)
                        OVER (partition by symbol, grp order by datetime),
                        historical_prices_marked.price_1w
                    )                                        as high,
                coalesce(
                        low,
                        first_value(close)
                        OVER (partition by symbol, grp order by datetime),
                        historical_prices_marked.price_1w
                    )                                        as low,
                coalesce(
                        close,
                        first_value(close)
                        OVER (partition by symbol, grp order by datetime),
                        historical_prices_marked.price_1w
                    )                                        as close,
                coalesce(volume, 0.0)                        as volume,
                adjustment_rate,
                first_value(adjustment_rate)
                OVER (partition by symbol order by datetime) as adjustment_rate2,
                daily_adjusted_close
         from (
                  select combined_intraday_prices.*,
                         coalesce(sum(case when combined_intraday_prices.close is not null then 1 end)
                                  over (partition by combined_intraday_prices.symbol order by datetime), 0) as grp,
                         case
                             when historical_prices.close > 0
                                 then historical_prices.adjusted_close / historical_prices.close
                         end                                                                                as adjustment_rate,
                         historical_prices.adjusted_close                                                   as daily_adjusted_close
                  from combined_intraday_prices
                           left join {{ ref('historical_prices') }}
                               on historical_prices.code = combined_intraday_prices.symbol
                                and historical_prices.date = combined_intraday_prices.datetime::date
              ) t
                  left join {{ ref('historical_prices_marked') }} using (symbol)
     ) t2
where t2.close is not null
