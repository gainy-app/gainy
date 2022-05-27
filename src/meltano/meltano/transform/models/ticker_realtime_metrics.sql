{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    tags = ["realtime"],
    post_hook=[
      pk('symbol'),
    ]
  )
}}

with latest_trading_day as
         (
             select t.symbol,
                    hpa_open.open   as open_price,
                    hpa_close.close as close_price,
                    t.min_datetime  as open_datetime,
                    t.max_datetime  as close_datetime,
                    t.sum_volume    as volume
             from (
                      select symbol,
                             max(datetime) as max_datetime,
                             min(datetime) as min_datetime,
                             sum(volume)      sum_volume
                      from {{ ref('chart') }}
                      where period = '1d'
                      group by symbol
                  ) t
                      join {{ ref('historical_prices_aggregated') }} hpa_close
                           on hpa_close.symbol = t.symbol
                               and hpa_close.period = '3min'
                               and hpa_close.datetime = t.max_datetime
                      join {{ ref('historical_prices_aggregated') }} hpa_open
                           on hpa_open.symbol = t.symbol
                               and hpa_open.period = '3min'
                               and hpa_open.datetime = t.min_datetime
         ),
     previous_trading_day as
         (
             (
                 select historical_prices_aggregated.symbol,
                        historical_prices_aggregated.adjusted_close
                 from (
                          select historical_prices_aggregated.symbol,
                                 period,
                                 max(historical_prices_aggregated.datetime) as datetime
                          from {{ ref('historical_prices_aggregated') }}
                                   left join latest_trading_day
                                             on historical_prices_aggregated.symbol = latest_trading_day.symbol
                                                 and historical_prices_aggregated.datetime < latest_trading_day.open_datetime::date
                                                 and historical_prices_aggregated.datetime >= latest_trading_day.open_datetime::date - interval '1 week'
                                   left join {{ ref('ticker_options_monitored') }}
                                             on ticker_options_monitored.contract_name = historical_prices_aggregated.symbol
                                   left join {{ ref('base_tickers') }}
                                             on base_tickers.symbol = historical_prices_aggregated.symbol
                          where period = '1d'
                            and (ticker_options_monitored.contract_name is not null
                              or base_tickers.type != 'crypto')
                          group by historical_prices_aggregated.symbol, historical_prices_aggregated.period
                      ) t
                          join {{ ref('historical_prices_aggregated') }} using (symbol, datetime, period)
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
     latest_datapoint as
         (
             select symbol,
                    datetime,
                    adjusted_close
             from (
                      select symbol,
                             period,
                             max(datetime) as datetime
                      from {{ ref('historical_prices_aggregated') }}
                      where period = '1d'
                      group by symbol, period
                  ) t
                      join {{ ref('historical_prices_aggregated') }} using (symbol, period, datetime)
         )
select symbol,
       coalesce(latest_trading_day.close_datetime, latest_datapoint.datetime)              as time,
       coalesce(latest_trading_day.close_price, latest_datapoint.adjusted_close)           as actual_price,
       coalesce(latest_trading_day.close_price - previous_trading_day.adjusted_close, 0.0) as absolute_daily_change,
       case
           when latest_trading_day.close_price is null
               then 0.0
           when previous_trading_day.adjusted_close > 0
               then (latest_trading_day.close_price / previous_trading_day.adjusted_close) - 1
           end                                                                             as relative_daily_change,
       coalesce(latest_trading_day.volume, 0.0)::double precision                          as daily_volume,
       latest_datapoint.adjusted_close                                                     as last_known_price,
       latest_datapoint.datetime                                                           as last_known_price_datetime,
       previous_trading_day.adjusted_close                                                 as previous_day_close_price
from latest_datapoint
         left join previous_trading_day using (symbol)
         left join latest_trading_day using (symbol)
