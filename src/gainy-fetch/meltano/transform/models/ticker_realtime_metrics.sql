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
                                         first_value(close)
                                         over (partition by symbol rows between current row and unbounded following) as open_price,
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
select latest_trading_day.symbol,
       latest_trading_day.close_datetime                                    as time,
       latest_trading_day.close_price                                       as actual_price,
       latest_trading_day.close_price - latest_trading_day.open_price       as absolute_daily_change,
       (latest_trading_day.close_price / latest_trading_day.open_price) - 1 as relative_daily_change,
       latest_trading_day.volume::double precision                          as daily_volume
from latest_trading_day