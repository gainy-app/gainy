{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('symbol, time'),
      index(this, 'id', true),
      'create index if not exists "symbol__time_3min" ON {{ this }} (symbol, time_3min)',
      'create index if not exists "symbol__time_15min" ON {{ this }} (symbol, time_15min)',
    ]
  )
}}


with week_trading_sessions as
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
                    (date_trunc('minute', time) - interval '1 minute' * mod(extract(minutes from time)::int, 3))::timestamp  as time_3min,
                    (date_trunc('minute', time) - interval '1 minute' * mod(extract(minutes from time)::int, 15))::timestamp as time_15min,
                    open,
                    high,
                    low,
                    close,
                    volume
             from {{ source('eod', 'eod_intraday_prices') }}
                      join {{ ref('base_tickers') }} using (symbol)
                      join week_trading_sessions
                           on (week_trading_sessions.exchange_name = base_tickers.exchange_canonical or
                               (base_tickers.exchange_canonical is null and
                                week_trading_sessions.country_name = base_tickers.country_name))
             where time >= week_trading_sessions.open_at
               and time < week_trading_sessions.close_at
         ),
{% if is_incremental() %}
     old_model_stats as
         (
             select symbol, max(time) as max_time
             from {{ this }}
             group by symbol
         )
{% else %}
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
                        else 1.0 -- TODO verify todays intraday prices after split are adjusted?
                        end as split_rate
             from daily_close_prices
                      left join {{ ref('historical_prices') }}
                                on historical_prices.code = daily_close_prices.symbol
                                    and historical_prices.date = daily_close_prices.date
                      join {{ source('eod', 'eod_intraday_prices') }} using (symbol, time)
         )
{% endif %}
select symbol,
       date,
       time,
       time_3min,
       time_15min,
       open,
       high,
       low,
       close,
       volume,

{% if is_incremental() %}
       close as adjusted_close,
{% else %}
       close * split_rate as adjusted_close,
{% endif %}

       (symbol || '_' || time) as id
from raw_intraday_prices

{% if is_incremental() %}
left join old_model_stats using (symbol)
where old_model_stats.max_time is null or raw_intraday_prices.time > max_time
{% else %}
         left join daily_adjustment_rate using (symbol, date)
{% endif %}
