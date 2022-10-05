{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('symbol, date'),
      index(this, 'id', true),
      'create unique index if not exists "symbol__date_year__date" ON {{ this }} (symbol, date_year, date)',
      'create unique index if not exists "symbol__date_month__date" ON {{ this }} (symbol, date_month, date)',
      'create unique index if not exists "date_week__symbol__date" ON {{ this }} (date_week, symbol, date)',
    ]
  )
}}


-- Execution Time: 152932.965 ms
with
{% if is_incremental() %}
old_model_stats as (select max(updated_at) as max_updated_at from {{ this }}),
{% endif %}
polygon_crypto_tickers as
    (
        select symbol
        from {{ ref('tickers') }}
        left join {{ source('eod', 'eod_historical_prices') }} on eod_historical_prices.code = tickers.symbol
        where eod_historical_prices.code is null
    ),
latest_stock_price_date as
    (
        SELECT code as symbol, max(date)::date as date
        from {{ source('eod', 'eod_historical_prices') }}
        group by code
    ),
next_trading_session as
    (
        select distinct on (symbol) symbol, exchange_schedule.date
        from latest_stock_price_date
                 join {{ ref('base_tickers') }} using (symbol)
                 left join {{ ref('exchange_schedule') }}
                           on (exchange_schedule.exchange_name =
                               base_tickers.exchange_canonical or
                               (base_tickers.exchange_canonical is null and
                                exchange_schedule.country_name = base_tickers.country_name))
        where exchange_schedule.date > latest_stock_price_date.date
        order by symbol, exchange_schedule.date
    ),
stocks_with_split as
    (
        select symbol as code,
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
        SELECT eod_historical_prices.code,
               eod_historical_prices.date,
               case when close > 0 then adjusted_close / close end as split_rate,
               stocks_with_split.split_from::numeric /
               stocks_with_split.split_to::numeric                 as split_rate_next_day,
               eod_historical_prices.open,
               eod_historical_prices.high,
               eod_historical_prices.low,
               eod_historical_prices.adjusted_close,
               eod_historical_prices.close,
               eod_historical_prices.volume,
               eod_historical_prices._sdc_batched_at
        from {{ source('eod', 'eod_historical_prices') }}

                 left join stocks_with_split using (code)
        where eod_historical_prices.date >= eod_historical_prices.first_date
    ),
latest_day_split_rate as
    (
        SELECT eod_historical_prices.code,
               first_value(adjusted_close / close) over (partition by code order by date desc) as latest_day_split_rate
        from {{ source('eod', 'eod_historical_prices') }}
                 join (
                          SELECT eod_historical_prices.code,
                                 max(eod_historical_prices.date) as date
                          from {{ source('eod', 'eod_historical_prices') }}
                          where close > 0
                          group by code
                      ) t using (code, date)
    ),
prev_split as
    (
        select code, max(date) as date
        from {{ source('eod', 'eod_historical_prices') }}
        where abs(close - eod_historical_prices.adjusted_close) > 1e-3
        group by code
    ),
wrongfully_adjusted_prices as
    (
        -- in case we get partially adjusted prices:
        -- 8.45	8.45
        -- 0.173	0.173
        -- 0.1908	0.1908
        -- in this case we need to adjust the rows from this subquery twice
        select code, date
        from (
                 select code,
                        date,
                        adjusted_close,
                        lag(adjusted_close) over (partition by code order by date) as prev_adjusted_close
                 from {{ source('eod', 'eod_historical_prices') }}
                 where date::date > now() - interval '1 week'
             ) t
        where adjusted_close > 0
          and prev_adjusted_close > 0
          and (adjusted_close / prev_adjusted_close > 2 or prev_adjusted_close / adjusted_close > 2)
    ),
all_rows as
    (
    SELECT prices_with_split_rates.code,
           case
               -- if there is no split tomorrow - just use the data from eod
               when split_rate_next_day is null
                   then adjusted_close
               -- latest split period, already adjusted
               when prices_with_split_rates.date > prev_split.date and wrongfully_adjusted_prices.code is not null
                   then adjusted_close
               -- latest split period, so we ignore split_rate and use 1.0
               when prices_with_split_rates.date > prev_split.date and wrongfully_adjusted_prices.code is null
                   then split_rate_next_day * close
               -- if there is split tomorrow, but the prices are already adjusted
               when abs(latest_day_split_rate - split_rate_next_day) < 1e-3
                   then adjusted_close
               else prices_with_split_rates.split_rate * split_rate_next_day * close
               end                                                                        as adjusted_close,
           case
               -- latest split period, already adjusted
               when prices_with_split_rates.date > prev_split.date and wrongfully_adjusted_prices.code is not null
                   then adjusted_close / split_rate_next_day
               else prices_with_split_rates.close
               end                                                                        as close,
           case
               when split_rate_next_day is null
                   then _sdc_batched_at
               when prices_with_split_rates.date > prev_split.date and wrongfully_adjusted_prices.code is not null
                   then _sdc_batched_at
               else now()
               end                                                                        as _sdc_batched_at,
           prices_with_split_rates.date,
           prices_with_split_rates.high,
           prices_with_split_rates.low,
           prices_with_split_rates.open,
           prices_with_split_rates.volume
    from prices_with_split_rates
             left join latest_day_split_rate using (code)
             left join prev_split using (code)
             left join wrongfully_adjusted_prices
                       on wrongfully_adjusted_prices.code = prices_with_split_rates.code
                           and wrongfully_adjusted_prices.date = prices_with_split_rates.date
)
select code                                      as symbol,
       (code || '_' || date)                     as id,
       substr(date, 0, 5)                        as date_year,
       (substr(date, 0, 8) || '-01')::timestamp  as date_month,
       date_trunc('week', date::date)::timestamp as date_week,
       adjusted_close,
       close,
       date::date,
       high,
       low,
       open,
       volume,
       all_rows._sdc_batched_at                  as updated_at
from all_rows
{% if is_incremental() %}
    left join old_model_stats on true
    where all_rows._sdc_batched_at >= old_model_stats.max_updated_at or old_model_stats.max_updated_at is null
{% endif %}

union all

SELECT contract_name                                                   as code,
       (contract_name || '_' || to_timestamp(t / 1000)::date)::varchar as id,
       extract(year from to_timestamp(t / 1000))::varchar              as date_year,
       date_trunc('month', to_timestamp(t / 1000))::timestamp          as date_month,
       date_trunc('week', to_timestamp(t / 1000))::timestamp           as date_week,
       c                                                               as adjusted_close,
       c                                                               as close,
       to_timestamp(t / 1000)::date                                    as date,
       h                                                               as high,
       l                                                               as low,
       o                                                               as open,
       v                                                               as volume,
       polygon_options_historical_prices._sdc_batched_at               as updated_at
from {{ source('polygon', 'polygon_options_historical_prices') }}
join {{ ref('ticker_options_monitored') }} using (contract_name)
{% if is_incremental() %}
    left join old_model_stats on true
    where polygon_options_historical_prices._sdc_batched_at >= old_model_stats.max_updated_at or old_model_stats.max_updated_at is null
{% endif %}

union all

SELECT polygon_crypto_tickers.symbol                                          as code,
       (polygon_crypto_tickers.symbol || '_' || to_timestamp(t / 1000)::date) as id,
       extract(year from to_timestamp(t / 1000))::varchar                     as date_year,
       date_trunc('month', to_timestamp(t / 1000))::timestamp                 as date_month,
       date_trunc('week', to_timestamp(t / 1000))::timestamp                  as date_week,
       c                                                                      as adjusted_close,
       c                                                                      as close,
       to_timestamp(t / 1000)::date                                           as date,
       h                                                                      as high,
       l                                                                      as low,
       o                                                                      as open,
       v                                                                      as volume,
       polygon_crypto_historical_prices._sdc_batched_at                       as updated_at
from polygon_crypto_tickers
         join {{ source('polygon', 'polygon_crypto_historical_prices') }}
              on polygon_crypto_historical_prices.symbol = regexp_replace(polygon_crypto_tickers.symbol, '.CC$', 'USD')
{% if is_incremental() %}
    left join old_model_stats on true
    where polygon_crypto_historical_prices._sdc_batched_at >= old_model_stats.max_updated_at or old_model_stats.max_updated_at is null
{% endif %}
