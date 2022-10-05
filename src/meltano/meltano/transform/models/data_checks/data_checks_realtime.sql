{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('id'),
      'delete from {{this}}
        using (select period, max(updated_at) as max_updated_at from {{this}} group by period) dc_stats
        where dc_stats.period = {{this}}.period
        and {{this}}.updated_at < dc_stats.max_updated_at',
    ]
  )
}}


with tickers_and_options as
         (
             select symbol, exchange_canonical, country_name, type
             from {{ ref('tickers') }}
             union all
             select contract_name, exchange_canonical, country_name, null as type
             from {{ ref('ticker_options_monitored') }}
                      join {{ ref('tickers') }} using (symbol)
         ),
     old_realtime_checks as
         (
             with latest_trading_day as
                      (
                          select eod_intraday_prices.symbol,
                                 max(close_at) as close_at,
                                 max(eod_intraday_prices.time) as time
                          from {{ source('eod', 'eod_intraday_prices') }}
                                   left join {{ ref('week_trading_sessions_static') }} using (symbol)
                          where week_trading_sessions_static is null
                            or (index = 0 and eod_intraday_prices.time between week_trading_sessions_static.open_at and week_trading_sessions_static.close_at)
                          group by eod_intraday_prices.symbol

                          union all

                          select polygon_intraday_prices.symbol,
                                 max(close_at) as close_at,
                                 max(polygon_intraday_prices.time) as time
                          from {{ source('polygon', 'polygon_intraday_prices') }}
                                   left join {{ ref('week_trading_sessions_static') }} using (symbol)
                          where week_trading_sessions_static is null
                            or (index = 0 and polygon_intraday_prices.time between week_trading_sessions_static.open_at and week_trading_sessions_static.close_at)
                          group by polygon_intraday_prices.symbol
                      )
             select symbol,
                    'old_realtime_metrics' as code,
                    'realtime' as period,
                    'Ticker ' || symbol || ' has old realtime metrics.' as message
             from tickers_and_options
                      left join {{ ref('ticker_realtime_metrics') }} using (symbol)
                      join latest_trading_day using (symbol)
             where ticker_realtime_metrics.time < least(latest_trading_day.close_at, latest_trading_day.time) - interval '20 minutes'
                or (ticker_realtime_metrics is null and latest_trading_day is not null)
                or (latest_trading_day.close_at is null and latest_trading_day is not null)
             group by symbol

             union all

             select symbol,
                    'old_realtime_chart' as code,
                    'realtime' as period,
                    'Ticker ' || symbol || ' has old or no ' || json_agg(period) || ' chart.' as message
             from tickers_and_options
                      join (select period from (values ('1d'), ('1w')) t (period)) periods on true
                      left join (
                                    select symbol,
                                           '1d' as period
                                    from {{ ref('historical_prices_aggregated_3min') }}
                                             join latest_trading_day using (symbol)
                                    where datetime > least(latest_trading_day.close_at, latest_trading_day.time) - interval '30 minutes'
                                    group by symbol

                                    union all

                                    select symbol,
                                           '1w' as period
                                    from {{ ref('historical_prices_aggregated_15min') }}
                                             join latest_trading_day using (symbol)
                                    where datetime > least(latest_trading_day.close_at, latest_trading_day.time) - interval '30 minutes'
                                    group by symbol
                                ) c using (symbol, period)
             where c is null
             group by symbol
         ),
     old_realtime_prices_eod as
         (
             with previous_trading_day_intraday_prices as
                      (
                          select symbol,
                                 type,
                                 date_trunc('minute', eod_intraday_prices.time) -
                                 interval '1 minute' *
                                 mod(extract(minutes from eod_intraday_prices.time)::int, 15) - open_at as time
                          from {{ ref('base_tickers') }}
                                   join {{ ref('week_trading_sessions_static') }} using (symbol)
                                   join {{ source('eod', 'eod_intraday_prices') }} using (symbol)
                          where (base_tickers.exchange_canonical in ('NYSE', 'NASDAQ') or
                                 (base_tickers.exchange_canonical is null and base_tickers.country_name = 'United States') or
                                 (base_tickers.exchange_canonical is null and base_tickers.country_name is null))
                            and week_trading_sessions_static.index = 1
                            and eod_intraday_prices.time between open_at and close_at - interval '1 second'
                      ),
                  previous_trading_day_intraday_prices_unique_symbols as
                      (
                          select symbol,
                                 type,
                                 time
                          from previous_trading_day_intraday_prices
                          group by symbol, type, time
                      ),
                  previous_trading_day_intraday_prices_stats as
                      (
                          select type,
                                 time,
                                 count(*) as cnt
                          from previous_trading_day_intraday_prices
                          group by type, time
                      ),
                  latest_trading_day_intraday_prices as
                      (
                          select symbol,
                                 type,
                                 date_trunc('minute', eod_intraday_prices.time) -
                                 interval '1 minute' *
                                 mod(extract(minutes from eod_intraday_prices.time)::int, 15) - open_at as time,
                                 open_at
                          from {{ ref('base_tickers') }}
                                   join {{ ref('week_trading_sessions_static') }} using (symbol)
                                   join {{ source('eod', 'eod_intraday_prices') }} using (symbol)
                          where (base_tickers.exchange_canonical in ('NYSE', 'NASDAQ') or
                                 (base_tickers.exchange_canonical is null and base_tickers.country_name = 'United States') or
                                 (base_tickers.exchange_canonical is null and base_tickers.country_name is null))
                            and week_trading_sessions_static.index = 0
                            and eod_intraday_prices.time between open_at and close_at - interval '1 second'
                      ),
                  latest_trading_day_intraday_prices_stats as
                      (
                          select type,
                                 time,
                                 open_at,
                                 count(*) as cnt
                          from latest_trading_day_intraday_prices
                          group by type, time, open_at
                      ),
                  latest_diff as
                      (
                          select distinct on (
                              type
                              ) type,
                                time,
                                (latest_trading_day_intraday_prices_stats.cnt -
                                 previous_trading_day_intraday_prices_stats.cnt)::float /
                                previous_trading_day_intraday_prices_stats.cnt as diff
                          from previous_trading_day_intraday_prices_stats
                                   left join latest_trading_day_intraday_prices_stats using (type, time)
                          where latest_trading_day_intraday_prices_stats.open_at + time < now() - interval '30 minutes' -- Polygon delay is 15 minutes
                            and previous_trading_day_intraday_prices_stats.cnt > 0
                          order by type, time desc
                      )
             select previous_trading_day_intraday_prices_unique_symbols.symbol,
                    previous_trading_day_intraday_prices_unique_symbols.type
             from latest_diff
                      join previous_trading_day_intraday_prices_unique_symbols using (type, time)
                      left join latest_trading_day_intraday_prices using (symbol, time)
             where diff < -0.30 and latest_trading_day_intraday_prices is null
         ),
     old_realtime_prices_polygon as
         (
             with previous_trading_day_intraday_prices as
                      (
                          select symbol,
                                 type,
                                 date_trunc('minute', polygon_intraday_prices.time) -
                                 interval '1 minute' *
                                 mod(extract(minutes from polygon_intraday_prices.time)::int, 15) - open_at as time
                          from {{ ref('base_tickers') }}
                                   join {{ ref('week_trading_sessions_static') }} using (symbol)
                                   join {{ source('polygon', 'polygon_intraday_prices') }} using (symbol)
                          where (base_tickers.exchange_canonical in ('NYSE', 'NASDAQ') or
                                 (base_tickers.exchange_canonical is null and base_tickers.country_name = 'United States') or
                                 (base_tickers.exchange_canonical is null and base_tickers.country_name is null))
                            and week_trading_sessions_static.index = 1
                            and polygon_intraday_prices.time between open_at and close_at - interval '1 second'
                      ),
                  previous_trading_day_intraday_prices_unique_symbols as
                      (
                          select symbol,
                                 type,
                                 time
                          from previous_trading_day_intraday_prices
                          group by symbol, type, time
                      ),
                  previous_trading_day_intraday_prices_stats as
                      (
                          select type,
                                 time,
                                 count(*) as cnt
                          from previous_trading_day_intraday_prices
                          group by type, time
                      ),
                  latest_trading_day_intraday_prices as
                      (
                          select symbol,
                                 type,
                                 date_trunc('minute', polygon_intraday_prices.time) -
                                 interval '1 minute' *
                                 mod(extract(minutes from polygon_intraday_prices.time)::int, 15) - open_at as time,
                                 open_at
                          from {{ ref('base_tickers') }}
                                   join {{ ref('week_trading_sessions_static') }} using (symbol)
                                   join {{ source('polygon', 'polygon_intraday_prices') }} using (symbol)
                          where (base_tickers.exchange_canonical in ('NYSE', 'NASDAQ') or
                                 (base_tickers.exchange_canonical is null and base_tickers.country_name = 'United States') or
                                 (base_tickers.exchange_canonical is null and base_tickers.country_name is null))
                            and week_trading_sessions_static.index = 0
                            and polygon_intraday_prices.time between open_at and close_at - interval '1 second'
                      ),
                  latest_trading_day_intraday_prices_stats as
                      (
                          select type,
                                 time,
                                 open_at,
                                 count(*) as cnt
                          from latest_trading_day_intraday_prices
                          group by type, time, open_at
                      ),
                  latest_diff as
                      (
                          select distinct on (
                              type
                              ) type,
                                time,
                                (latest_trading_day_intraday_prices_stats.cnt -
                                 previous_trading_day_intraday_prices_stats.cnt)::float /
                                previous_trading_day_intraday_prices_stats.cnt as diff
                          from previous_trading_day_intraday_prices_stats
                                   left join latest_trading_day_intraday_prices_stats using (type, time)
                          where latest_trading_day_intraday_prices_stats.open_at + time < now() - interval '30 minutes' -- Polygon delay is 15 minutes
                            and previous_trading_day_intraday_prices_stats.cnt > 0
                          order by type, time desc
                      )
             select previous_trading_day_intraday_prices_unique_symbols.symbol,
                    previous_trading_day_intraday_prices_unique_symbols.type
             from latest_diff
                      join previous_trading_day_intraday_prices_unique_symbols using (type, time)
                      left join latest_trading_day_intraday_prices using (symbol, time)
             where diff < -0.30 and latest_trading_day_intraday_prices is null
         ),
     realtime_chart_diff_with_prev_point as
         (
             select symbol,
                    'Ticker ' || symbol || ' looks too volatile. '
                        || json_agg(json_build_array(period, datetime, diff)) as message
             from (
                      select *,
                             abs((adjusted_close - prev_adjusted_close) / least(adjusted_close, prev_adjusted_close)) as diff
                      from (
                               select symbol,
                                      '1d'                                                             as period,
                                      lag(adjusted_close) over (partition by symbol order by datetime) as prev_adjusted_close,
                                      lag(volume) over (partition by symbol order by datetime)         as prev_volume,
                                      datetime,
                                      adjusted_close,
                                      volume
                               from {{ ref('historical_prices_aggregated_3min') }}
{% if var('realtime') %}
                                    join {{ ref('week_trading_sessions') }} using (symbol)
                               where week_trading_sessions.index = 0 and historical_prices_aggregated_3min.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at
                                 and datetime > least(now(), week_trading_sessions.close_at) - interval '1 hour'
{% endif %}
                           ) t
                      where adjusted_close > 0
                        and prev_adjusted_close > 0
                  ) t
             where (diff > 0.02 and (volume + prev_volume) > 10000000)
                or (diff > 0.2 and (volume + prev_volume) > 1000000)
                or (diff > 0.6 and (volume + prev_volume) > 100000)
                or (diff > 1 and (volume + prev_volume) > 10000)
                or (diff > 2 and (volume + prev_volume) > 1000)
             group by symbol
         ),
     realtime_chart_diff_between_periods as
         (
             select symbol,
                    'Ticker ' || symbol || ' differs between 1d and 1w. diff: ' || diff as message
             from (
                       select historical_prices_aggregated_15min.symbol,
                              avg(abs(historical_prices_aggregated_15min.adjusted_close - historical_prices_aggregated_3min.adjusted_close) /
                                  historical_prices_aggregated_15min.adjusted_close) as diff
                       from {{ ref('historical_prices_aggregated_15min') }}
                            join {{ ref('historical_prices_aggregated_3min') }}
                                 on historical_prices_aggregated_3min.symbol = historical_prices_aggregated_15min.symbol
                                     and historical_prices_aggregated_3min.datetime = historical_prices_aggregated_15min.datetime + interval '12 minutes'
                       where historical_prices_aggregated_15min.adjusted_close > 0
                       group by historical_prices_aggregated_15min.symbol
                  ) t
             where diff > 0.01
         ),
{% if not var('realtime') %}
     realtime_chart_diff_with_historical as
         (
             select symbol,
                    'Ticker ' || symbol || ' has old realtime chart difference comparing to historical chart. '
                        || json_agg(json_build_array(period, datetime, diff)) as message
             from (
                      select *,
                             (realtime_daily_close_prices.adjusted_close - historical_prices.adjusted_close) /
                             historical_prices.adjusted_close as diff
                      from (
                               (
                                   select symbol,
                                          '1d'           as period,
                                          date,
                                          datetime,
                                          adjusted_close
                                   from {{ ref('historical_prices_aggregated_3min') }}
                                        join (
                                                  select symbol,
                                                         date,
                                                         '1d'           as period,
                                                         max(datetime) as datetime
                                                  from {{ ref('historical_prices_aggregated_3min') }}
                                                  group by symbol, date
                                             ) t using (symbol, date, datetime)
                               )
                               union all
                               (
                                   select symbol,
                                          '1w'           as period,
                                          date,
                                          datetime,
                                          adjusted_close
                                   from {{ ref('historical_prices_aggregated_15min') }}
                                        join (
                                                  select symbol,
                                                         date,
                                                         '1d'           as period,
                                                         max(datetime) as datetime
                                                  from {{ ref('historical_prices_aggregated_15min') }}
                                                  group by symbol, date
                                             ) t using (symbol, date, datetime)
                               )
                           ) realtime_daily_close_prices
                               join ( -- only tickers with realtime prices
                                         select symbol
                                         from {{ ref('historical_intraday_prices') }}
                                         group by symbol
                                    ) t using (symbol)
                               join {{ ref('historical_prices') }} using (symbol, date)
                      where historical_prices.adjusted_close > 0
                  ) t
             where abs(diff) > 0.1
             group by symbol
         ),
{% endif %}
     errors as
         (
             select symbol,
                    code,
                    period,
                    message
             from old_realtime_checks

             union all

             select symbol,
                    'old_realtime_prices_eod' as code,
                    'realtime' as period,
                    'Type ' || type || ' has old eod realtime prices.' as message
             from old_realtime_prices_eod

             union all

             select symbol,
                    'old_realtime_prices_polygon' as code,
                    'realtime' as period,
                    'Type ' || type || ' has old polygon realtime prices.' as message
             from old_realtime_prices_polygon

             union all

             select symbol,
                    'realtime_chart_diff_with_prev_point' as code,
                    'realtime' as period,
                    message
             from realtime_chart_diff_with_prev_point

             union all

             select symbol,
                    'realtime_chart_diff_between_periods' as code,
                    'realtime' as period,
                    message
             from realtime_chart_diff_between_periods

{% if not var('realtime') %}
             union all

             select symbol,
                    'realtime_chart_diff_with_historical' as code,
                    'daily' as period,
                    message
             from realtime_chart_diff_with_historical
{% endif %}
         )
select (code || '_' || symbol) as id,
       symbol,
       code,
       period,
       message,
       now() as updated_at
from errors
