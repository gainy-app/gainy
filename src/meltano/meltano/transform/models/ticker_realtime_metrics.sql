{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    tags = ["realtime"],
    post_hook=[
      index(this, 'symbol', true),
    ]
  )
}}

with latest_trading_day as
         (
             select chart_open.symbol,
                    chart_open.open   as open_price,
                    chart_close.close as close_price,
                    t.min_datetime    as open_datetime,
                    t.max_datetime    as close_datetime,
                    t.sum_volume      as volume
             from (
                      select symbol,
                             max(datetime) as max_datetime,
                             min(datetime) as min_datetime,
                             sum(volume)      sum_volume
                      from {{ ref('chart') }}
                      where period = '1d'
                      group by symbol
                  ) t
                      join {{ ref('chart') }} chart_close on chart_close.symbol = t.symbol and chart_close.period = '1d' and chart_close.datetime = t.max_datetime
                      join {{ ref('chart') }} chart_open on chart_open.symbol = t.symbol and chart_open.period = '1d' and chart_open.datetime = t.min_datetime
         ),
     previous_trading_day as
         (
             (
                 select historical_prices.code as symbol,
                        historical_prices.adjusted_close
                 from (
                          select historical_prices.code, max(historical_prices.date) as date
                          from {{ ref('historical_prices') }}
                                   join latest_trading_day
                                        on historical_prices.code = latest_trading_day.symbol and
                                           historical_prices.date < latest_trading_day.open_datetime::date
                                   join {{ ref('base_tickers') }} on base_tickers.symbol = historical_prices.code
                          where base_tickers.type != 'crypto'
                          group by historical_prices.code
                      ) t
                          join {{ ref('historical_prices') }}
                               on historical_prices.code = t.code and historical_prices.date = t.date
             )
             union all
             (
                 select latest_trading_day.symbol,
                        latest_trading_day.open_price
                 from latest_trading_day
                          join {{ ref('base_tickers') }} using (symbol)
                 where base_tickers.type = 'crypto'
             )
         ),
     latest_realtime_datapoint as
         (
             select eod_intraday_prices.symbol,
                    eod_intraday_prices.close,
                    eod_intraday_prices.time
             from (
                      select symbol,
                             max(time) as time
                      from {{ source('eod', 'eod_intraday_prices') }}
                      group by symbol
                  ) t
                      join {{ source('eod', 'eod_intraday_prices') }}
                           on eod_intraday_prices.symbol = t.symbol and eod_intraday_prices.time = t.time
         )
select latest_trading_day.symbol,
       latest_trading_day.close_datetime                                    as time,
       latest_trading_day.close_price                                       as actual_price,
       latest_trading_day.close_price - previous_trading_day.adjusted_close as absolute_daily_change,
       case
           when previous_trading_day.adjusted_close > 0
               then (latest_trading_day.close_price / previous_trading_day.adjusted_close) - 1
           end                                                              as relative_daily_change,
       latest_trading_day.volume::double precision                          as daily_volume,
       latest_realtime_datapoint.close::double precision                    as last_known_price,
       latest_realtime_datapoint.time::timestamp                            as last_known_price_datetime,
       previous_trading_day.adjusted_close                                  as previous_day_close_price
from latest_trading_day
         join previous_trading_day on previous_trading_day.symbol = latest_trading_day.symbol
         left join latest_realtime_datapoint on latest_realtime_datapoint.symbol = latest_trading_day.symbol
