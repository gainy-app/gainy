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
                          select symbol,
                                 close_at
                          from {{ ref('week_trading_sessions_static') }}
                          where index = 0
                      )
             select symbol,
                    'old_realtime_metrics' as code,
                    'realtime' as period,
                    'Ticker ' || symbol || ' has old realtime metrics.' as message
             from tickers_and_options
                      left join {{ ref('ticker_realtime_metrics') }} using (symbol)
                      join latest_trading_day using (symbol)
             where ticker_realtime_metrics.time < least(latest_trading_day.close_at, now()) - interval '20 minutes'
                or (ticker_realtime_metrics.symbol is null and latest_trading_day.symbol is not null)
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
                                             left join latest_trading_day using (symbol)
                                    where datetime > least(latest_trading_day.close_at, now()) - interval '30 minutes'
                                    group by symbol

                                    union all

                                    select symbol,
                                           '1w' as period
                                    from {{ ref('historical_prices_aggregated_15min') }}
                                             left join latest_trading_day using (symbol)
                                    where datetime > least(latest_trading_day.close_at, now()) - interval '40 minutes'
                                    group by symbol
                                ) c using (symbol, period)
             where c.symbol is null
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
             where diff < -0.30 and latest_trading_day_intraday_prices.symbol is null
         ),
     old_realtime_prices_polygon as
         (
             with previous_trading_day_intraday_prices as
                      (
                          select symbol,
                                 type,
                                 to_timestamp((polygon_intraday_prices_launchpad.t / 1000 / 60)::int * 60) -
                                 interval '1 minute' *
                                 mod((polygon_intraday_prices_launchpad.t / 1000 / 60)::int, 15) - open_at as time
                          from {{ ref('base_tickers') }}
                                   join {{ ref('week_trading_sessions_static') }} using (symbol)
                                   join {{ source('polygon', 'polygon_intraday_prices_launchpad') }} using (symbol)
                          where (base_tickers.exchange_canonical in ('NYSE', 'NASDAQ') or
                                 (base_tickers.exchange_canonical is null and base_tickers.country_name = 'United States') or
                                 (base_tickers.exchange_canonical is null and base_tickers.country_name is null))
                            and week_trading_sessions_static.index = 1
                            and polygon_intraday_prices_launchpad.t between open_at_t and close_at_t
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
                                 to_timestamp((polygon_intraday_prices_launchpad.t / 1000 / 60)::int * 60) -
                                 interval '1 minute' *
                                 mod((polygon_intraday_prices_launchpad.t / 1000 / 60)::int, 15) - open_at as time,
                                 open_at
                          from {{ ref('base_tickers') }}
                                   join {{ ref('week_trading_sessions_static') }} using (symbol)
                                   join {{ source('polygon', 'polygon_intraday_prices_launchpad') }} using (symbol)
                          where (base_tickers.exchange_canonical in ('NYSE', 'NASDAQ') or
                                 (base_tickers.exchange_canonical is null and base_tickers.country_name = 'United States') or
                                 (base_tickers.exchange_canonical is null and base_tickers.country_name is null))
                            and week_trading_sessions_static.index = 0
                            and polygon_intraday_prices_launchpad.t between extract(epoch from open_at) * 1000 and extract(epoch from close_at - interval '1 second') * 1000
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
             where diff < -0.30 and latest_trading_day_intraday_prices.symbol is null
         ),
     realtime_chart_diff_with_prev_point as
         (
             select symbol,
                    json_agg(json_build_array(period, datetime, diff))::text as message
             from (
                      select *,
                             abs((adjusted_close - prev_adjusted_close) / least(adjusted_close, prev_adjusted_close)) as diff
                      from (
                               select symbol,
                                      '1d'                                                                      as period,
                                      lag(adjusted_close) over (partition by symbol order by datetime)          as prev_adjusted_close,
                                      lag(volume * adjusted_close) over (partition by symbol order by datetime) as prev_volume,
                                      datetime,
                                      adjusted_close,
                                      volume * adjusted_close                                                   as volume
                               from {{ ref('historical_prices_aggregated_3min') }}
                           ) t
                      where adjusted_close > 0
                        and prev_adjusted_close > 0
                  ) t
                      left join {{ ref('ticker_risk_scores') }} using (symbol)
             where (diff > 1 and (volume + prev_volume) > 10000000 and coalesce(risk_score, 0) < 0.9)
                or (diff > 2.5 and (volume + prev_volume) > 100000)
                or (diff > 5 and (prev_adjusted_close > 1 or adjusted_close > 1))
             group by symbol
         ),
     realtime_chart_diff_between_periods as
         (
             select symbol,
                    json_agg(json_build_array('1d', '1w', datetime, diff))::text as message
             from (
                       select historical_prices_aggregated_15min.symbol,
                              historical_prices_aggregated_15min.datetime,
                              avg(abs(historical_prices_aggregated_15min.adjusted_close - historical_prices_aggregated_3min.adjusted_close) /
                                  historical_prices_aggregated_15min.adjusted_close) as diff
                       from {{ ref('historical_prices_aggregated_15min') }}
                                join {{ ref('historical_prices_aggregated_3min') }}
                                     on historical_prices_aggregated_3min.symbol = historical_prices_aggregated_15min.symbol
                                         and historical_prices_aggregated_3min.datetime = historical_prices_aggregated_15min.datetime + interval '12 minutes'
                       where historical_prices_aggregated_15min.adjusted_close > 0
                       group by historical_prices_aggregated_15min.symbol, historical_prices_aggregated_15min.datetime
                  ) t
             where diff > 0.01
             group by symbol
         ),
{% if not var('realtime') %}
     realtime_chart_diff_with_historical as
         (
             select symbol,
                    json_agg(json_build_array(period, datetime, diff))::text as message
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
     ticker_wrong_previous_day_close_price as
         (
             with ticker_daily_latest_chart_point as
                      (
                          select chart.*
                          from (
                                   select symbol, period, date, max(datetime) as datetime
                                   from {{ ref('chart') }}
                                            join {{ ref('week_trading_sessions_static') }} using (symbol, date)
                                   where index = 1 and period = '1w'
                                   group by symbol, period, date
                               ) t
                                   join {{ ref('chart') }} using (symbol, period, datetime)
                      )
             select symbol,
                    'Ticker ' || symbol || ' has wrong previous_day_close_price. ' ||
                    json_build_array(ticker_realtime_metrics.previous_day_close_price,
                                     ticker_daily_latest_chart_point.adjusted_close) as message
             from {{ ref('tickers') }}
                      left join {{ ref('ticker_realtime_metrics') }} using (symbol)
                      left join ticker_daily_latest_chart_point using (symbol)
             where ticker_realtime_metrics.symbol is null
                or ticker_daily_latest_chart_point.symbol is null
                or ticker_realtime_metrics.previous_day_close_price < 1e-12
                or (abs(ticker_daily_latest_chart_point.adjusted_close / ticker_realtime_metrics.previous_day_close_price - 1) > 0.2
                 and ticker_daily_latest_chart_point.volume > 0)
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
                    'realtime_chart_diff_between_periods' as code,
                    'realtime' as period,
                    message
             from realtime_chart_diff_between_periods

{% if not var('realtime') %}
             union all

             select symbol,
                    'realtime_chart_diff_with_prev_point' as code,
                    'daily' as period,
                    message
             from realtime_chart_diff_with_prev_point

             union all

             select symbol,
                    'realtime_chart_diff_with_historical' as code,
                    'daily' as period,
                    message
             from realtime_chart_diff_with_historical

             union all

             select symbol,
                    'ticker_wrong_previous_day_close_price' as code,
                    'daily' as period,
                    message
             from ticker_wrong_previous_day_close_price
{% endif %}
         )
select (code || '_' || symbol) as id,
       symbol,
       code,
       period,
       message,
       now() as updated_at
from errors
