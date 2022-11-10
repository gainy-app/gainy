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
                    hpa_open.open                     as open_price,
                    hpa_close.adjusted_close          as close_price,
                    t.min_datetime                    as open_datetime,
                    t.max_datetime + interval '3 min' as close_datetime,
                    t.max_date                        as close_date,
                    t.sum_volume                      as volume
             from (
                      select symbol,
                             max(datetime) as max_datetime,
                             max(date)     as max_date,
                             min(datetime) as min_datetime,
                             sum(volume)      sum_volume
                      from {{ ref('chart') }}
                      where period = '1d'
                      group by symbol
                  ) t
                      join {{ ref('historical_prices_aggregated_3min') }} hpa_close
                           on hpa_close.symbol = t.symbol
                               and hpa_close.datetime = t.max_datetime
                      join {{ ref('historical_prices_aggregated_3min') }} hpa_open
                           on hpa_open.symbol = t.symbol
                               and hpa_open.datetime = t.min_datetime
         ),
     previous_trading_day as
         (
             (
                 select historical_prices_aggregated_1d.symbol,
                        historical_prices_aggregated_1d.adjusted_close
                 from (
                          select historical_prices_aggregated_1d.symbol,
                                 max(historical_prices_aggregated_1d.datetime) as datetime
                          from {{ ref('historical_prices_aggregated_1d') }}
                                   join latest_trading_day
                                        on historical_prices_aggregated_1d.symbol = latest_trading_day.symbol
                                            and historical_prices_aggregated_1d.datetime < latest_trading_day.open_datetime::date
                                            and historical_prices_aggregated_1d.datetime >= latest_trading_day.open_datetime::date - interval '1 week'
                                   left join {{ ref('ticker_options_monitored') }}
                                             on ticker_options_monitored.contract_name = historical_prices_aggregated_1d.symbol
                                   left join {{ ref('base_tickers') }}
                                             on base_tickers.symbol = historical_prices_aggregated_1d.symbol
                          where ticker_options_monitored.contract_name is not null
                             or base_tickers.type != 'crypto'
                          group by historical_prices_aggregated_1d.symbol
                      ) t
                          join {{ ref('historical_prices_aggregated_1d') }} using (symbol, datetime)
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
             with chart_symbols as
                      (
                         select symbol
                         from {{ ref('base_tickers') }}
                         union all
                         select contract_name as symbol
                         from {{ ref('ticker_options_monitored') }}
                      ),
                  max_intraday as
                      (
                          select symbol, max(time) as time, interval '1 minute' as period_interval
                          from {{ ref('historical_intraday_prices') }}
                          group by symbol
                      ),
                  max_historical as
                      (
                          select symbol, max(date) as date, interval '1 day' as period_interval
                          from {{ ref('historical_prices') }}
                          group by symbol
                      )
             select chart_symbols.symbol,
                    coalesce(
                                max_intraday.time + max_intraday.period_interval,
                                max_historical.date + max_historical.period_interval
                        ) as datetime,
                    coalesce(
                            historical_intraday_prices.date,
                            historical_prices.date
                        ) as date,
                    coalesce(
                            historical_intraday_prices.adjusted_close,
                            historical_prices.adjusted_close
                        ) as adjusted_close
             from chart_symbols
                      left join max_intraday using (symbol)
                      left join max_historical using (symbol)

                      left join {{ ref('historical_intraday_prices') }}
                                on historical_intraday_prices.symbol = max_intraday.symbol
                                    and historical_intraday_prices.time = max_intraday.time
                      left join {{ ref('historical_prices') }}
                                on historical_prices.symbol = max_historical.symbol
                                    and historical_prices.date = max_historical.date
         )
select symbol,
       coalesce(latest_trading_day.close_datetime, latest_datapoint.datetime)              as time,
       coalesce(latest_trading_day.close_date, latest_datapoint.date)                      as date,
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
