{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('symbol, date'),
      index('id', true),
      'create unique index if not exists "symbol__date_year__date" ON {{ this }} (symbol, date_year, date)',
      'create unique index if not exists "symbol__date_month__date" ON {{ this }} (symbol, date_month, date)',
      'create unique index if not exists "date_week__symbol__date" ON {{ this }} (date_week, symbol, date)',
    ]
  )
}}


-- Execution Time: 450490.199 ms
with
polygon_symbols as materialized
    (
        select symbol
        from {{ source('polygon', 'polygon_stocks_historical_prices') }}
        where t >= (select max(t) from {{ source('polygon', 'polygon_stocks_historical_prices') }})
        group by symbol
    ),
raw_eod_historical_prices as
    (
        select code                                      as symbol,
               date::date,
               substr(date, 0, 5)                        as date_year,
               (substr(date, 0, 8) || '-01')::timestamp  as date_month,
               date_trunc('week', date::date)::timestamp as date_week,
               open,
               high,
               low,
               close,
               adjusted_close,
               volume,
               'eod'                                     as source,
               _sdc_batched_at                           as updated_at
        from {{ source('eod', 'eod_historical_prices') }}
                 left join polygon_symbols
                           on polygon_symbols.symbol = eod_historical_prices.code
        where polygon_symbols.symbol is null
          and eod_historical_prices.date >= eod_historical_prices.first_date
          and eod_historical_prices.adjusted_close > 0
),
raw_polygon_stocks_historical_prices as
    (
        select symbol,
               to_timestamp(t / 1000)::date                           as date,
               extract(year from to_timestamp(t / 1000))::varchar     as date_year,
               date_trunc('month', to_timestamp(t / 1000))::timestamp as date_month,
               date_trunc('week', to_timestamp(t / 1000))::timestamp  as date_week,
               o                                                      as open,
               h                                                      as high,
               l                                                      as low,
               c                                                      as close,
               c                                                      as adjusted_close,
               v                                                      as volume,
               'polygon'                                              as source,
               _sdc_batched_at                                        as updated_at
        from {{ source('polygon', 'polygon_stocks_historical_prices') }}
                 join polygon_symbols using (symbol)
),
raw_historical_prices as materialized
    (
        select symbol,
               date,
               date_year,
               date_month,
               date_week,
               open,
               high,
               low,
               close,
               adjusted_close,
               volume,
               source,
               updated_at
        from raw_polygon_stocks_historical_prices

        union all

        select symbol,
               date,
               date_year,
               date_month,
               date_week,
               open,
               high,
               low,
               close,
               adjusted_close,
               volume,
               source,
               updated_at
        from raw_eod_historical_prices
),
polygon_crypto_tickers as
    (
        select symbol
        from {{ ref('tickers') }}
                 left join raw_historical_prices using (symbol)
        where raw_historical_prices.symbol is null
    ),
latest_stock_price_date as
    (
        SELECT symbol, max(date) as date
        from raw_historical_prices
        where adjusted_close > 0
        group by symbol
    ),
next_trading_session as
    (
        select distinct on (symbol) symbol, exchange_schedule.date::date
        from latest_stock_price_date
                 join {{ ref('base_tickers') }} using (symbol)
                 left join {{ ref('exchange_schedule') }}
                           on (exchange_schedule.exchange_name =
                               base_tickers.exchange_canonical or
                               (base_tickers.exchange_canonical is null and
                                exchange_schedule.country_name = base_tickers.country_name))
        where exchange_schedule.date > latest_stock_price_date.date::date
        order by symbol, exchange_schedule.date
    ),
stocks_with_split as
    (
        select symbol,
               split_to,
               split_from
        from {{ ref('base_tickers') }}
                 left join next_trading_session using (symbol)
                 left join {{ source('polygon', 'polygon_stock_splits') }}
                           on polygon_stock_splits.execution_date::date = next_trading_session.date
                               and polygon_stock_splits.ticker = base_tickers.symbol
                               and polygon_stock_splits._sdc_extracted_at > (select max(_sdc_extracted_at) from {{ source('polygon', 'polygon_stock_splits') }}) - interval '1 minute'
    ),
prices_with_split_rates as
    (
        SELECT raw_historical_prices.symbol,
               raw_historical_prices.date,
               raw_historical_prices.date_year,
               raw_historical_prices.date_month,
               raw_historical_prices.date_week,
               case
                   when source = 'eod' and close > 0
                       then adjusted_close / close
                   end                                             as split_rate,
               case
                   when source = 'eod'
                       then stocks_with_split.split_from::numeric / stocks_with_split.split_to::numeric
                   end                                             as split_rate_next_day,
               raw_historical_prices.open,
               raw_historical_prices.high,
               raw_historical_prices.low,
               raw_historical_prices.adjusted_close,
               raw_historical_prices.close,
               raw_historical_prices.volume,
               raw_historical_prices.updated_at
        from raw_historical_prices
                 left join stocks_with_split using (symbol)
    ),
latest_day_split_rate as
    (
        SELECT raw_eod_historical_prices.symbol,
               first_value(adjusted_close / close) over wnd as latest_day_split_rate
        from raw_eod_historical_prices
                 join latest_stock_price_date using (symbol, date)
            window wnd as (partition by symbol order by raw_eod_historical_prices.date desc)
),
prev_split as
    (
        select symbol, max(date) as date
        from raw_eod_historical_prices
        where abs(close - adjusted_close) > 1e-3
        group by symbol
),
wrongfully_adjusted_prices as
    (
        -- in case we get partially adjusted prices:
        -- 8.45	8.45
        -- 0.173	0.173
        -- 0.1908	0.1908
        -- in this case we need to adjust the rows from this subquery twice
        select symbol, date
        from (
                 select symbol,
                        date,
                        adjusted_close,
                        lag(adjusted_close) over (partition by symbol order by date) as prev_adjusted_close
                 from raw_eod_historical_prices
                 where date::date > now() - interval '1 week'
             ) t
        where adjusted_close > 0
          and prev_adjusted_close > 0
          and (adjusted_close / prev_adjusted_close > 2 or prev_adjusted_close / adjusted_close > 2)
    ),
all_rows as
    (
    SELECT prices_with_split_rates.symbol,
           case
               -- if there is no split tomorrow - just use the data from eod
               when split_rate_next_day is null
                   then adjusted_close
               -- latest split period, already adjusted
               when prices_with_split_rates.date > prev_split.date and wrongfully_adjusted_prices.symbol is not null
                   then adjusted_close
               -- latest split period, so we ignore split_rate and use 1.0
               when prices_with_split_rates.date > prev_split.date and wrongfully_adjusted_prices.symbol is null
                   then split_rate_next_day * close
               -- if there is split tomorrow, but the prices are already adjusted
               when abs(latest_day_split_rate - split_rate_next_day) < 1e-3
                   then adjusted_close
               else prices_with_split_rates.split_rate * split_rate_next_day * close
               end                                                                        as adjusted_close,
           case
               -- latest split period, already adjusted
               when prices_with_split_rates.date > prev_split.date and wrongfully_adjusted_prices.symbol is not null
                   then adjusted_close / split_rate_next_day
               else prices_with_split_rates.close
               end                                                                        as close,
           case
               when split_rate_next_day is null
                   then updated_at
               when prices_with_split_rates.date > prev_split.date and wrongfully_adjusted_prices.symbol is not null
                   then updated_at
               else now()
               end as updated_at,
           prices_with_split_rates.date,
           prices_with_split_rates.date_year,
           prices_with_split_rates.date_month,
           prices_with_split_rates.date_week,
           prices_with_split_rates.high,
           prices_with_split_rates.low,
           prices_with_split_rates.open,
           prices_with_split_rates.volume
    from prices_with_split_rates
             left join latest_day_split_rate using (symbol)
             left join prev_split using (symbol)
             left join wrongfully_adjusted_prices
                       on wrongfully_adjusted_prices.symbol = prices_with_split_rates.symbol
                           and wrongfully_adjusted_prices.date = prices_with_split_rates.date
)
select t.*
from (
         select symbol,
                (symbol || '_' || date) as id,
                date_year,
                date_month,
                date_week,
                adjusted_close,
                case
                    when lag(adjusted_close) over wnd > 0
                        then coalesce(adjusted_close::numeric / (lag(adjusted_close) over wnd)::numeric - 1, 0)
                    end                 as relative_daily_gain,
                close,
                date::date,
                high,
                low,
                open,
                volume::numeric,
                all_rows.updated_at
         from all_rows
             window wnd as (partition by symbol order by date rows between 1 preceding and current row)

         union all

         SELECT contract_name                                                   as symbol,
                (contract_name || '_' || to_timestamp(t / 1000)::date)::varchar as id,
                extract(year from to_timestamp(t / 1000))::varchar              as date_year,
                date_trunc('month', to_timestamp(t / 1000))::timestamp          as date_month,
                date_trunc('week', to_timestamp(t / 1000))::timestamp           as date_week,
                c                                                               as adjusted_close,
                case
                    when lag(c) over wnd > 0
                        then coalesce(c::numeric / (lag(c) over wnd)::numeric - 1, 0)
                    end                                                         as relative_daily_gain,
                c                                                               as close,
                to_timestamp(t / 1000)::date                                    as date,
                h                                                               as high,
                l                                                               as low,
                o                                                               as open,
                v::numeric                                                      as volume,
                polygon_options_historical_prices._sdc_batched_at               as updated_at
         from {{ source('polygon', 'polygon_options_historical_prices') }}
                  join {{ ref('ticker_options_monitored') }} using (contract_name)
             window wnd as (partition by contract_name order by t rows between 1 preceding and current row)

         union all

         SELECT polygon_crypto_tickers.symbol,
                (polygon_crypto_tickers.symbol || '_' || to_timestamp(t / 1000)::date) as id,
                extract(year from to_timestamp(t / 1000))::varchar                     as date_year,
                date_trunc('month', to_timestamp(t / 1000))::timestamp                 as date_month,
                date_trunc('week', to_timestamp(t / 1000))::timestamp                  as date_week,
                c                                                                      as adjusted_close,
                case
                    when lag(c) over wnd > 0
                        then coalesce(c::numeric / (lag(c) over wnd)::numeric - 1, 0)
                    end                                                                as relative_daily_gain,
                c                                                                      as close,
                to_timestamp(t / 1000)::date                                           as date,
                h                                                                      as high,
                l                                                                      as low,
                o                                                                      as open,
                v::numeric                                                             as volume,
                polygon_crypto_historical_prices._sdc_batched_at                       as updated_at
         from polygon_crypto_tickers
                  join {{ source('polygon', 'polygon_crypto_historical_prices') }}
                       on polygon_crypto_historical_prices.symbol = regexp_replace(polygon_crypto_tickers.symbol, '.CC$', 'USD')
             window wnd as (partition by polygon_crypto_tickers.symbol order by t rows between 1 preceding and current row)
      ) t
{% if is_incremental() %}
         left join {{ this }} old_data using (symbol, date)
where old_data.symbol is null
   or (old_data.relative_daily_gain is null and t.relative_daily_gain is not null)
   or abs(t.adjusted_close - old_data.adjusted_close) > 1e-3
   or abs(t.relative_daily_gain - old_data.relative_daily_gain) > 1e-3
{% endif %}
