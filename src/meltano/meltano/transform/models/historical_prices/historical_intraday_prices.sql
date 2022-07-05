{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('symbol, time'),
      index(this, 'id', true),
    ]
  )
}}


with
{% if is_incremental() %}
max_updated_at as (select code, max(date) as max_date from {{ this }} group by code),
{% endif %}
     week_trading_sessions as
         (
             select exchange_name,
                    null          as country_name,
                    date,
                    min(open_at)  as open_at,
                    max(close_at) as close_at
             from {{ ref('exchange_schedule') }}
             where open_at between now() - interval '1 week' and now()
             group by exchange_name, date

             union all

             select null          as exchange_name,
                    country_name,
                    date,
                    min(open_at)  as open_at,
                    max(close_at) as close_at
             from {{ ref('exchange_schedule') }}
             where open_at between now() - interval '1 week' and now()
             group by country_name, date
         ),
     raw_intraday_prices as
         (
             select symbol,
                    date,
                    time,
                    open,
                    high,
                    low,
                    close
             from {{ source('eod', 'eod_intraday_prices') }}
                      join {{ ref('base_tickers') }} using (symbol)
                      join week_trading_sessions
                           on (week_trading_sessions.exchange_name = base_tickers.exchange_canonical or
                               (base_tickers.exchange_canonical is null and
                                week_trading_sessions.country_name = base_tickers.country_name))
             where time >= week_trading_sessions.open_at
               and time < week_trading_sessions.close_at
         ),
     daily_close_prices as
         (
             select symbol,
                    date,
                    max(time) as time
             from raw_intraday_prices
             group by symbol, date
         ),
     daily_adjustment_rate as
         (
             select symbol,
                    daily_close_prices.date,
                    case
                        when historical_prices.close > 0
                            and abs(eod_intraday_prices.close - historical_prices.close) <
                                abs(eod_intraday_prices.close - historical_prices.adjusted_close)
                            and abs(historical_prices.adjusted_close / historical_prices.close - 1) > 1e-2
                            then historical_prices.adjusted_close / historical_prices.close
                        else 1.0
                        end as split_rate
             from daily_close_prices
                      join {{ ref('historical_prices') }}
                           on historical_prices.code = daily_close_prices.symbol
                               and historical_prices.date = daily_close_prices.date
                      join {{ source('eod', 'eod_intraday_prices') }} using (symbol, time)
         )
select symbol,
       date,
       time,
       open,
       high,
       low,
       close,
       close * split_rate      as adjusted_close,
       (symbol || '_' || time) as id
from raw_intraday_prices
         left join daily_adjustment_rate using (symbol, date)
