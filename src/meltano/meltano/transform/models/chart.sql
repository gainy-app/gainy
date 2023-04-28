{{
  config(
    materialized = "view",
  )
}}


(
    select historical_prices_aggregated_3min.symbol,
           week_trading_sessions.date,
           historical_prices_aggregated_3min.datetime,
           '1d'::varchar as period,
           historical_prices_aggregated_3min.open,
           historical_prices_aggregated_3min.high,
           historical_prices_aggregated_3min.low,
           historical_prices_aggregated_3min.close,
           historical_prices_aggregated_3min.adjusted_close,
           historical_prices_aggregated_3min.volume,
           historical_prices_aggregated_3min.updated_at
    from {{ ref('historical_prices_aggregated_3min') }}
             join {{ ref('week_trading_sessions') }} using (symbol)
    where week_trading_sessions.index = 0
      and historical_prices_aggregated_3min.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at - interval '1 microsecond'
)

union all

(
    select historical_prices_aggregated_15min.symbol,
           week_trading_sessions.date,
           historical_prices_aggregated_15min.datetime,
           '1w'::varchar as period,
           historical_prices_aggregated_15min.open,
           historical_prices_aggregated_15min.high,
           historical_prices_aggregated_15min.low,
           historical_prices_aggregated_15min.close,
           historical_prices_aggregated_15min.adjusted_close,
           historical_prices_aggregated_15min.volume,
           historical_prices_aggregated_15min.updated_at
    from {{ ref('historical_prices_aggregated_15min') }}
             join {{ ref('week_trading_sessions') }} using (symbol)
    where week_trading_sessions.date >= now()::date - interval '1 week'
      and week_trading_sessions.index >= 0
      and historical_prices_aggregated_15min.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at - interval '1 microsecond'
)

union all

(
    select distinct on (
        symbol, date
        ) symbol,
          date,
          datetime,
          '1m'::varchar as period,
          open,
          high,
          low,
          close,
          adjusted_close,
          volume,
          updated_at
    from (
             select symbol,
                    date,
                    datetime,
                    open,
                    high,
                    low,
                    close,
                    adjusted_close,
                    volume,
                    updated_at,
                    0 as priority
             from {{ ref('historical_prices_aggregated_1d') }}
             where datetime >= now()::date - interval '1 month'

             union all

             select symbol,
                    date::date,
                    date::timestamp as datetime,
                    null            as open,
                    null            as high,
                    null            as low,
                    null            as close,
                    actual_price    as adjusted_close,
                    null            as volume,
                    updated_at,
                    1               as priority
             from {{ ref('ticker_realtime_metrics') }}
         ) t
    order by symbol, date, priority
)

union all

(
    select distinct on (
        symbol, date
        ) symbol,
          date,
          datetime,
          '3m'::varchar as period,
          open,
          high,
          low,
          close,
          adjusted_close,
          volume,
          updated_at
    from (
             select symbol,
                    date,
                    datetime,
                    open,
                    high,
                    low,
                    close,
                    adjusted_close,
                    volume,
                    updated_at,
                    0 as priority
             from {{ ref('historical_prices_aggregated_1d') }}
             where datetime >= now()::date - interval '3 month'

             union all

             select symbol,
                    date::date,
                    date::timestamp as datetime,
                    null            as open,
                    null            as high,
                    null            as low,
                    null            as close,
                    actual_price    as adjusted_close,
                    null            as volume,
                    updated_at,
                    1               as priority
             from {{ ref('ticker_realtime_metrics') }}
         ) t
    order by symbol, date, priority
)

union all

(
    select symbol,
           date,
           datetime,
           period,
           open,
           high,
           low,
           close,
           adjusted_close,
           volume,
           updated_at
    from (
             select distinct on (
                 symbol, date
                 ) *
             from (
                      select symbol,
                             date,
                             datetime,
                             open,
                             high,
                             low,
                             close,
                             adjusted_close,
                             volume,
                             updated_at,
                             0 as priority
                      from {{ ref('historical_prices_aggregated_1d') }}
                      where datetime >= now()::date - interval '1 year'

                      union all

                      select symbol,
                             date::date,
                             date::timestamp as datetime,
                             null            as open,
                             null            as high,
                             null            as low,
                             null            as close,
                             actual_price    as adjusted_close,
                             null            as volume,
                             updated_at,
                             1               as priority
                      from {{ ref('ticker_realtime_metrics') }}
                  ) t
             order by symbol, date, priority
         ) t
             join (
                      with symbols as
                               (
                                   select symbol
                                   from {{ ref('historical_prices_aggregated_1d') }}
                                   group by symbol
                               )
                      select symbol, '1y'::varchar as period
                      from symbols

                      union all

                      select symbol, period::varchar
                      from (
                               select symbol from {{ ref('historical_prices_aggregated_1w') }} group by symbol having count(*) < 3
                           ) t
                               join symbols using (symbol)
                               join (
                                        values ('all'), ('5y')
                                    ) periods(period) on true
                  ) periods using (symbol)
)

union all

(
    select symbol,
           date,
           datetime,
           period,
           open,
           high,
           low,
           close,
           adjusted_close,
           volume,
           updated_at
    from (
             select distinct on (
                 symbol, date
                 ) *
             from (
                      select symbol,
                             datetime::date as date,
                             datetime,
                             open,
                             high,
                             low,
                             close,
                             adjusted_close,
                             volume,
                             updated_at,
                             1              as priority
                      from {{ ref('historical_prices_aggregated_1w') }}
                      where historical_prices_aggregated_1w.datetime >= now()::date - interval '5 year'

                      union all

                      select symbol,
                             date_trunc('week', date)::date      as date,
                             date_trunc('week', date)::timestamp as datetime,
                             null                                as open,
                             null                                as high,
                             null                                as low,
                             null                                as close,
                             actual_price                        as adjusted_close,
                             null                                as volume,
                             updated_at,
                             0                                   as priority
                      from {{ ref('ticker_realtime_metrics') }}
                  ) t
             order by symbol, date, priority
         ) t
             join (
                      with symbols as
                               (
                                   select symbol
                                   from {{ ref('historical_prices_aggregated_1w') }}
                                   group by symbol
                                   having count(*) >= 3
                               )
                      select symbol, '5y'::varchar as period
                      from symbols

                      union all

                      select symbol, 'all'::varchar as period
                      from (
                               select symbol from {{ ref('historical_prices_aggregated_1m') }} group by symbol having count(*) < 3
                           ) t
                               join symbols using (symbol)
                  ) periods using (symbol)
)

union all

(
    with symbols as
             (
                 select symbol
                 from {{ ref('historical_prices_aggregated_1m') }}
                 group by symbol
                 having count(*) >= 3
             )
    select distinct on (
        symbol, date
        ) symbol,
          date,
          datetime,
          'all'::varchar                as period,
          open,
          high,
          low,
          close,
          adjusted_close,
          volume,
          updated_at
    from (
             select symbol,
                    datetime::date as date,
                    datetime,
                    open,
                    high,
                    low,
                    close,
                    adjusted_close,
                    volume,
                    updated_at,
                    1              as priority
             from {{ ref('historical_prices_aggregated_1m') }}

             union all

             select symbol,
                    date_trunc('month', date)::date      as date,
                    date_trunc('month', date)::timestamp as datetime,
                    null                                 as open,
                    null                                 as high,
                    null                                 as low,
                    null                                 as close,
                    actual_price                         as adjusted_close,
                    null                                 as volume,
                    updated_at,
                    0                                    as priority
             from {{ ref('ticker_realtime_metrics') }}
         ) t
             join symbols using (symbol)
    order by symbol, date, priority
)
