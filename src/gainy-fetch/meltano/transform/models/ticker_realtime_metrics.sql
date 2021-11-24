{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    incremental_strategy = 'insert_overwrite',
    post_hook=[
      index(this, 'symbol', true),
    ]
  )
}}

with latest_daily_prices as
         (
             select distinct on (symbol) symbol,
                                         time,
                                         case when adjusted_close > 0 then adjusted_close end as adjusted_close
             from historical_prices_aggregated
             where period = '1d' and time > now() - interval '1 week'
             order by symbol, time desc
         )
select distinct on (
    historical_prices_aggregated.symbol
    ) historical_prices_aggregated.symbol,
      historical_prices_aggregated.time,
      historical_prices_aggregated.adjusted_close                               as actual_price,
      historical_prices_aggregated.adjusted_close -
      latest_daily_prices.adjusted_close                                        as absolute_daily_change,
      (historical_prices_aggregated.adjusted_close / latest_daily_prices.adjusted_close) -
      1                                                                         as relative_daily_change,
      (sum(historical_prices_aggregated.volume::numeric)
       OVER (partition by historical_prices_aggregated.symbol
           order by historical_prices_aggregated.time desc
           rows between current row and unbounded following))::double precision as daily_volume
from {{ ref('historical_prices_aggregated') }}
         join latest_daily_prices on latest_daily_prices.symbol = historical_prices_aggregated.symbol
where historical_prices_aggregated.period = '15min'
order by historical_prices_aggregated.symbol, historical_prices_aggregated.time desc