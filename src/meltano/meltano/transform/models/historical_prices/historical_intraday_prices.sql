{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('symbol, time'),
      index('id', true),
      index(['time', 'source']),
      'create index if not exists "symbol__time_3min" ON {{ this }} (symbol, time_3min)',
      'create index if not exists "symbol__time_15min" ON {{ this }} (symbol, time_15min)',
    ]
  )
}}


with polygon_symbols as
         (
             select symbol
             from {{ source('polygon', 'polygon_intraday_prices_launchpad') }}
             group by symbol
         ),
     raw_eod_intraday_prices as
         (
             select eod_intraday_prices.symbol,
                    t.date,
                    time,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    'eod' as source
             from {{ source('eod', 'eod_intraday_prices') }}
                      join (
                               select symbol, date, open_at, close_at
                               from {{ ref('week_trading_sessions_static') }}
                                        left join polygon_symbols using (symbol)
                               where polygon_symbols.symbol is null
{% if var('realtime') %}
                                 and week_trading_sessions_static.index = 0
{% else %}
                                 and week_trading_sessions_static.index >= 0
{% endif %}
                           ) t using (symbol)
             where time >= open_at
               and time < close_at
         ),
     raw_polygon_intraday_prices as
         (
             select symbol,
                    week_trading_sessions_static.date,
                    to_timestamp(t / 1000) as time,
                    o                      as open,
                    h                      as high,
                    l                      as low,
                    c                      as close,
                    v                      as volume,
                    'polygon'              as source
             from {{ source('polygon', 'polygon_intraday_prices_launchpad') }}
                      join {{ ref('week_trading_sessions_static') }} using (symbol)
             where t >= week_trading_sessions_static.open_at_t
               and t < week_trading_sessions_static.close_at_t
{% if var('realtime') %}
               and week_trading_sessions_static.index = 0
{% else %}
               and week_trading_sessions_static.index >= 0
{% endif %}
         ),
     raw_intraday_prices as materialized
         (
             select symbol,
                    date,
                    date_trunc('minute', time)::timestamp                                                                    as time,
                    (date_trunc('minute', time) - interval '1 minute' * mod(extract(minutes from time)::int, 3))::timestamp  as time_3min,
                    (date_trunc('minute', time) - interval '1 minute' * mod(extract(minutes from time)::int, 15))::timestamp as time_15min,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    source
             from (
                     select symbol, date, time, open, high, low, close, volume, source
                     from raw_polygon_intraday_prices

                     union all

                     select symbol, date, time, open, high, low, close, volume, source
                     from raw_eod_intraday_prices
                  ) t
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
                            and base_tickers.type != 'crypto'
                            and abs(historical_prices.adjusted_close / raw_intraday_prices.close - 1) > 1e-2
                            then historical_prices.adjusted_close / raw_intraday_prices.close
                        else 1.0 -- TODO verify todays intraday prices after split are adjusted?
                        end as split_rate
             from daily_close_prices
                      join {{ ref('base_tickers') }} using (symbol)
                      left join {{ ref('historical_prices') }} using (symbol, date)
                      join raw_intraday_prices using (symbol, time)
{% endif %}
         ),
    data as
        (
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
                    source,

             {% if not var('realtime') %}
                    (close * coalesce(split_rate, 1))::double precision as adjusted_close,
             {% else %}
                    close::double precision                             as adjusted_close,
             {% endif %}
                    0                                                   as priority

             from raw_intraday_prices

             {% if not var('realtime') %}
                      left join daily_adjustment_rate using (symbol, date)

             union all

             select symbol,
                    date,
                    close_at::timestamp              as time,
                    close_at - interval '3 minutes'  as time_3min,
                    close_at - interval '15 minutes' as time_15min,
                    open::double precision,
                    high::double precision,
                    low::double precision,
                    close::double precision,
                    0::double precision              as volume,
                    'historical_prices'              as source,
                    adjusted_close::double precision,
                    1                                as priority
             from {{ ref('historical_prices_aggregated_1d') }}
                      join {{ ref('week_trading_sessions_static') }} using (symbol, date)
             where close_at < now()
               and week_trading_sessions_static.index >= 0
             {% endif %}
        )

select t.*
from (
         select distinct on (
             symbol, time
             ) symbol,
               date,
               time::timestamp,
               time_3min::timestamp,
               time_15min::timestamp,
               open::double precision,
               high::double precision,
               low::double precision,
               close::double precision,
               volume::double precision,
               adjusted_close::double precision,
               source,
               now()::timestamp        as updated_at,
               (symbol || '_' || time) as id
         from data
         order by symbol, time, priority desc
     ) t

{% if is_incremental() %}
         left join {{ this }} old_data using (symbol, time)
where old_data is null or abs(old_data.adjusted_close - t.adjusted_close) > 1e-3
{% endif %}
