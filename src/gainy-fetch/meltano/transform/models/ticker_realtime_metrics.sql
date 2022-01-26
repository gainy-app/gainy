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
                                         over (partition by symbol rows between current row and unbounded following) as close_price,
                                         last_value(datetime)
                                         over (partition by symbol rows between current row and unbounded following) as close_datetime,
                                         sum(volume)
                                         over (partition by symbol rows between current row and unbounded following) as volume
             from {{ ref('chart') }}
             where period = '1d'
             order by symbol, datetime
         )
select distinct on (
    latest_trading_day.symbol
    ) latest_trading_day.symbol,
      latest_trading_day.close_datetime                                                  as time,
      latest_trading_day.close_price                                                     as actual_price,
      latest_trading_day.close_price - historical_prices_aggregated.adjusted_close       as absolute_daily_change,
      (latest_trading_day.close_price / historical_prices_aggregated.adjusted_close) - 1 as relative_daily_change,
      latest_trading_day.volume::double precision                                        as daily_volume
from latest_trading_day
         join {{ ref('historical_prices_aggregated') }}
              on historical_prices_aggregated.period = '1d'
                  and historical_prices_aggregated.symbol = latest_trading_day.symbol
                  and historical_prices_aggregated.datetime < latest_trading_day.close_datetime::date
                  and historical_prices_aggregated.datetime > latest_trading_day.close_datetime - interval '1 week'
order by latest_trading_day.symbol, historical_prices_aggregated.datetime desc
