{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('code, date'),
      index(this, 'id', true),
      'create unique index if not exists "code__date_year__date" ON {{ this }} (code, date_year, date)',
      'create unique index if not exists "code__date_month__date" ON {{ this }} (code, date_month, date)',
      'create unique index if not exists "date_week__code__date" ON {{ this }} (date_week, code, date)',
    ]
  )
}}


-- Execution Time: 152932.965 ms
with
{% if is_incremental() %}
max_updated_at as (select code, max(date) as max_date from {{ this }} group by code),
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
                           on polygon_stock_splits.execution_date::date =
                              next_trading_session.date
                               and polygon_stock_splits.ticker = base_tickers.symbol
    ),
prices_with_split_rates as
    (
        SELECT eod_historical_prices.code,
               eod_historical_prices.date,
               case when close > 0 then adjusted_close / close end as split_rate,
               stocks_with_split.split_to::numeric /
               stocks_with_split.split_from::numeric               as split_rate_next_day,
               eod_historical_prices.open,
               eod_historical_prices.high,
               eod_historical_prices.low,
               eod_historical_prices.adjusted_close,
               eod_historical_prices.close,
               eod_historical_prices.volume
        from {{ source('eod', 'eod_historical_prices') }}
                 left join stocks_with_split using (code)
    ),
prev_split as
    (
        select code, max(date) as date
        from {{ source('eod', 'eod_historical_prices') }}
        where abs(close - eod_historical_prices.adjusted_close) > 1e-3
        group by code
    )
SELECT prices_with_split_rates.code,
       (prices_with_split_rates.code || '_' || prices_with_split_rates.date)::varchar as id,
       substr(prices_with_split_rates.date, 0, 5)                                     as date_year,
       (substr(prices_with_split_rates.date, 0, 8) || '-01')::timestamp               as date_month,
       date_trunc('week', prices_with_split_rates.date::date)::timestamp              as date_week,
       case
           -- if there is no split tomorrow - just use the data from eod
           when split_rate_next_day is null
               then adjusted_close
           -- latest split period, so we ignore split_rate and use 1.0
           when prices_with_split_rates.date > prev_split.date
               then split_rate_next_day * close
           else prices_with_split_rates.split_rate * split_rate_next_day * close
           end                                                                        as adjusted_close,
       prices_with_split_rates.close,
       prices_with_split_rates.date::date,
       prices_with_split_rates.high,
       prices_with_split_rates.low,
       prices_with_split_rates.open,
       prices_with_split_rates.volume
from prices_with_split_rates
         left join prev_split using (code)

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
       v                                                               as volume
from {{ source('polygon', 'polygon_options_historical_prices') }}
join {{ ref('ticker_options_monitored') }} using (contract_name)
{% if is_incremental() %}
    left join max_updated_at on max_updated_at.code = contract_name
    where _sdc_batched_at >= max_updated_at.max_date or max_updated_at.max_date is null
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
       v                                                                      as volume
from polygon_crypto_tickers
         join {{ source('polygon', 'polygon_crypto_historical_prices') }}
              on polygon_crypto_historical_prices.symbol = regexp_replace(polygon_crypto_tickers.symbol, '.CC$', 'USD')
{% if is_incremental() %}
    left join max_updated_at on max_updated_at.code = polygon_crypto_tickers.symbol
    where _sdc_batched_at >= max_updated_at.max_date or max_updated_at.max_date is null
{% endif %}
