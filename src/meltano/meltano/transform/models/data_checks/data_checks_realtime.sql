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


with latest_trading_day as
         (
             select distinct on (exchange_name, country_name) *
             from {{ ref('exchange_schedule') }}
             where open_at < now()
             order by exchange_name, country_name, date desc
         ),
     previous_trading_day as
         (
             select distinct on (exchange_schedule.exchange_name, exchange_schedule.country_name) exchange_schedule.*
             from {{ ref('exchange_schedule') }}
                      join latest_trading_day
                           on (latest_trading_day.exchange_name = exchange_schedule.exchange_name
                               or (latest_trading_day.exchange_name is null
                                   and latest_trading_day.country_name = exchange_schedule.country_name))
             where exchange_schedule.date < latest_trading_day.date
             order by exchange_schedule.exchange_name, exchange_schedule.country_name, exchange_schedule.date desc
         ),
     tickers_and_options as
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
             with indexed_ticker_schedules as
                      (
                          select symbol,
                                 exchange_schedule.date,
                                 exchange_schedule.open_at,
                                 exchange_schedule.close_at - interval '1 millisecond'      as close_at,
                                 row_number() over (partition by symbol order by date desc) as idx,
                                 row_number() over (partition by symbol order by date)      as idx_inv
                          from tickers_and_options
                                   left join {{ ref('exchange_schedule')}}
                                             on (tickers_and_options.exchange_canonical = exchange_schedule.exchange_name
                                                 or (tickers_and_options.exchange_canonical is null and
                                                     tickers_and_options.country_name = exchange_schedule.country_name))
                          where open_at between now() - interval '1 week' and now()
                            and type != 'crypto'

                          union all

                          select symbol,
                                 (dd - interval '1 day')::date                            as date,
                                 dd - interval '1 day'                                    as open_at,
                                 dd - interval '1 millisecond'                            as close_at,
                                 row_number() over (partition by symbol order by dd desc) as idx,
                                 row_number() over (partition by symbol order by dd)      as idx_inv
                          from generate_series(now() - interval '6 days', now(), interval '1 day') dd
                                   join tickers_and_options on type = 'crypto'
                      ),
                  latest_trading_day as
                      (
                          select t.time,
                                 indexed_ticker_schedules.*
                          from (
                                   select eod_intraday_prices.symbol,
                                          indexed_ticker_schedules.date,
                                          max(eod_intraday_prices.time) as time
                                   from {{ source('eod', 'eod_intraday_prices') }}
                                            join indexed_ticker_schedules
                                                 on indexed_ticker_schedules.symbol = eod_intraday_prices.symbol
                                                     and
                                                    eod_intraday_prices.time between indexed_ticker_schedules.open_at and indexed_ticker_schedules.close_at
                                   group by eod_intraday_prices.symbol, indexed_ticker_schedules.date
                               ) t
                                   join indexed_ticker_schedules using (symbol, date)
                      )
             select symbol,
                    'old_realtime_metrics' as code,
                    'realtime' as period,
                    'Ticker ' || symbol || ' has old realtime metrics.' as message
             from tickers_and_options
                      left join {{ ref('ticker_realtime_metrics') }} using (symbol)
                      join latest_trading_day using (symbol)
             where ticker_realtime_metrics.time < least(latest_trading_day.close_at, latest_trading_day.time) - interval '20 minutes'
                or ticker_realtime_metrics is null
             group by symbol

             union all

             select symbol,
                    'old_realtime_chart_' || period as code,
                    'realtime' as period,
                    'Ticker ' || symbol || ' has old or no ' || json_agg(period) || ' chart.' as message
             from tickers_and_options
                      join latest_trading_day using (symbol)
                      join (select period from (values ('1d'), ('1w')) t (period)) periods on true
                      left join (
                                    select symbol,
                                           period,
                                           max(chart.datetime) as datetime
                                    from {{ ref('chart') }}
                                    where chart.period in ('1d', '1w')
                                    group by symbol, period
                                ) chart_1d_latest using (symbol, period)
             where chart_1d_latest.datetime < least(latest_trading_day.close_at, latest_trading_day.time) - interval '20 minutes'
                or (chart_1d_latest is null and period = '1d' and idx = 1)
                or (chart_1d_latest is null and period = '1w')
             group by symbol, period
         ),
     old_realtime_prices as
         (
             with previous_trading_day_intraday_prices as
                      (
                          with crypto_open_at as
                                   (

                                       select (
                                                  values (date_trunc('minute', now() - interval '2 days') -
                                                          interval '1 minute' *
                                                          mod(extract(minutes from now() - interval '2 days')::int, 15))
                                              ) as datetime
                                   )
                          select symbol,
                                 type,
                                 date_trunc('minute', eod_intraday_prices.time) -
                                 interval '1 minute' *
                                 mod(extract(minutes from eod_intraday_prices.time)::int, 15) -
                                 coalesce(open_at, crypto_open_at.datetime) as time
                          from {{ ref('base_tickers') }}
                                   left join previous_trading_day
                                             on (previous_trading_day.exchange_name = base_tickers.exchange_canonical
                                                 or (base_tickers.exchange_canonical is null
                                                     and previous_trading_day.country_name = base_tickers.country_name))
                                   left join {{ source('eod', 'eod_intraday_prices')}} using (symbol)
                                   join crypto_open_at on true
                          where (base_tickers.exchange_canonical in ('NYSE', 'NASDAQ') or
                                 (base_tickers.exchange_canonical is null and base_tickers.country_name = 'United States') or
                                 (base_tickers.exchange_canonical is null and base_tickers.country_name is null))
                            and eod_intraday_prices.time between coalesce(open_at, crypto_open_at.datetime) and coalesce(
                                      close_at - interval '1 second', crypto_open_at.datetime + interval '1 day')
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
                          with crypto_open_at as
                                   (

                                       select (
                                                  values (date_trunc('minute', now() - interval '1 day') -
                                                          interval '1 minute' *
                                                          mod(extract(minutes from now() - interval '1 day')::int, 15))
                                              ) as datetime
                                   )
                          select symbol,
                                 type,
                                 date_trunc('minute', eod_intraday_prices.time) -
                                 interval '1 minute' *
                                 mod(extract(minutes from eod_intraday_prices.time)::int, 15) -
                                 coalesce(open_at, crypto_open_at.datetime)                    as time,
                                 coalesce(latest_trading_day.open_at, crypto_open_at.datetime) as open_at
                          from {{ ref('base_tickers') }}
                                   left join latest_trading_day
                                             on (latest_trading_day.exchange_name = base_tickers.exchange_canonical
                                                 or (base_tickers.exchange_canonical is null
                                                     and latest_trading_day.country_name = base_tickers.country_name))
                                   left join {{ source('eod', 'eod_intraday_prices')}} using (symbol)
                                   join crypto_open_at on true
                          where (base_tickers.exchange_canonical in ('NYSE', 'NASDAQ') or
                                 (base_tickers.exchange_canonical is null and base_tickers.country_name = 'United States') or
                                 (base_tickers.exchange_canonical is null and base_tickers.country_name is null))
                            and eod_intraday_prices.time between coalesce(open_at, crypto_open_at.datetime) and coalesce(
                                      close_at - interval '1 second', crypto_open_at.datetime + interval '1 day')
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

                               union all

                               select symbol,
                                      '1w'                                                             as period,
                                      lag(adjusted_close) over (partition by symbol order by datetime) as prev_adjusted_close,
                                      lag(volume) over (partition by symbol order by datetime)         as prev_volume,
                                      datetime,
                                      adjusted_close,
                                      volume
                               from {{ ref('historical_prices_aggregated_15min') }}
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
                                   select distinct on (
                                       symbol, datetime::date
                                       ) symbol,
                                         '1d'           as period,
                                         datetime::date as date,
                                         datetime,
                                         adjusted_close
                                   from {{ ref('historical_prices_aggregated_3min') }}
                                   order by symbol, datetime::date, datetime desc
                               )
                               union all
                               (
                                   select distinct on (
                                       symbol, datetime::date
                                       ) symbol,
                                         '1w'           as period,
                                         datetime::date as date,
                                         datetime,
                                         adjusted_close
                                   from {{ ref('historical_prices_aggregated_15min') }}
                                   order by symbol, datetime::date, datetime desc
                               )
                           ) realtime_daily_close_prices
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
                    'old_realtime_prices' as code,
                    'realtime' as period,
                    'Type ' || type || ' has old realtime prices.' as message
             from old_realtime_prices

             union all

             select symbol,
                    'realtime_chart_diff_with_prev_point' as code,
                    'realtime' as period,
                    message
             from realtime_chart_diff_with_prev_point

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
