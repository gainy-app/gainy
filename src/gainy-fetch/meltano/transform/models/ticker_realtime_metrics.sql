{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    post_hook=[
      index(this, 'symbol', true),
    ]
  )
}}

with latest_trading_day as
         (
             select distinct on (symbol) symbol,
                                         last_value(close)
                                         over (partition by symbol order by datetime rows between current row and unbounded following) as close_price,
                                         last_value(datetime)
                                         over (partition by symbol order by datetime rows between current row and unbounded following) as close_datetime,
                                         sum(volume)
                                         over (partition by symbol order by datetime rows between current row and unbounded following) as volume
             from {{ ref('chart') }}
             where period = '1d'
             order by symbol, datetime
         ),
     latest_realtime_datapoint as
         (
             select distinct on (symbol) symbol,
                                         close,
                                         time
             from {{ source('eod', 'eod_intraday_prices') }}
             where time > now() - interval '1 week'
             order by symbol, time desc
         )
select distinct on (
    latest_trading_day.symbol
    ) latest_trading_day.symbol,
      latest_trading_day.close_datetime                                            as time,
      latest_trading_day.close_price                                               as actual_price,
      latest_trading_day.close_price - historical_prices_aggregated.adjusted_close as absolute_daily_change,
      case
          when historical_prices_aggregated.adjusted_close > 0
              then (latest_trading_day.close_price / historical_prices_aggregated.adjusted_close) - 1
          end                                                                      as relative_daily_change,
      latest_trading_day.volume::double precision                                  as daily_volume,
      latest_realtime_datapoint.close                                              as last_known_price,
      latest_realtime_datapoint.time::timestamp                                    as last_known_price_datetime,
      historical_prices_aggregated.adjusted_close                                  as previous_day_close_price
from latest_trading_day
         join {{ ref('historical_prices_aggregated') }}
              on historical_prices_aggregated.period = '1d'
                  and historical_prices_aggregated.symbol = latest_trading_day.symbol
                  and historical_prices_aggregated.datetime < latest_trading_day.close_datetime::date
                  and historical_prices_aggregated.datetime > latest_trading_day.close_datetime - interval '1 week'
         left join latest_realtime_datapoint on latest_realtime_datapoint.symbol = latest_trading_day.symbol
         left join {{ ref('base_tickers') }} on base_tickers.symbol = latest_trading_day.symbol
order by latest_trading_day.symbol, historical_prices_aggregated.datetime desc