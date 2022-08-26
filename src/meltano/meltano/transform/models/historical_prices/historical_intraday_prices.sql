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


with eod_symbols as materialized
         (
             select symbol,
                    week_trading_sessions.date
             from {{ source('eod', 'eod_intraday_prices') }}
                      join {{ ref('week_trading_sessions') }} using (symbol)
             where eod_intraday_prices.time >= week_trading_sessions.open_at
               and eod_intraday_prices.time < week_trading_sessions.close_at
             group by symbol, week_trading_sessions.date
         ),
     raw_polygon_intraday_prices as
         (
             select polygon_intraday_prices.symbol,
                    week_trading_sessions.date,
                    time,
                    open,
                    high,
                    low,
                    close,
                    volume
             from {{ source('polygon', 'polygon_intraday_prices') }}
                      join {{ ref('week_trading_sessions') }} using (symbol)
                      left join eod_symbols using (symbol, date)
             where eod_symbols is null
               and time >= week_trading_sessions.open_at
               and time < week_trading_sessions.close_at
         ),
     raw_eod_intraday_prices as
         (
             select eod_intraday_prices.symbol,
                    week_trading_sessions.date,
                    time,
                    open,
                    high,
                    low,
                    close,
                    volume
             from {{ source('eod', 'eod_intraday_prices') }}
                      join {{ ref('week_trading_sessions') }} using (symbol)
                      join eod_symbols using (symbol, date)
             where time >= week_trading_sessions.open_at
               and time < week_trading_sessions.close_at
         ),
     raw_intraday_prices as materialized
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
             from (
                     select symbol, date, time, open, high, low, close, volume
                     from raw_polygon_intraday_prices

                     union all

                     select symbol, date, time, open, high, low, close, volume
                     from raw_eod_intraday_prices
                  ) t
{% if is_incremental() %}
         ),
     old_model_stats as
         (
             select symbol, max(time) as max_time
             from {{ this }}
             group by symbol
{% endif %}
{% if not var('realtime') %}
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
                        when raw_intraday_prices.close > 0
                            and abs(historical_prices.adjusted_close / raw_intraday_prices.close - 1) > 1e-2
                            then historical_prices.adjusted_close / raw_intraday_prices.close
                        else 1.0 -- TODO verify todays intraday prices after split are adjusted?
                        end as split_rate
             from daily_close_prices
                      left join {{ ref('historical_prices') }} using (symbol, date)
                      join raw_intraday_prices using (symbol, time)
{% endif %}
         )
select symbol,
       date,
       time,
       time_3min,
       time_15min,
       open::double precision,
       high::double precision,
       low::double precision,
       close::double precision,
       volume::double precision,

{% if not var('realtime') %}
       (close * split_rate)::double precision as adjusted_close,
{% else %}
       close::double precision as adjusted_close,
{% endif %}

       now()::timestamp as updated_at,
       (symbol || '_' || time) as id
from raw_intraday_prices

{% if is_incremental() %}
         left join old_model_stats using (symbol)
{% endif %}

{% if not var('realtime') %}
         left join daily_adjustment_rate using (symbol, date)
{% endif %}

{% if is_incremental() %}
where old_model_stats.max_time is null or raw_intraday_prices.time > max_time
{% endif %}
{% if is_incremental() and not var('realtime') %}
   or abs(split_rate - 1) > 1e-3
{% endif %}
