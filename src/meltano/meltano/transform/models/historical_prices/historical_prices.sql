{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('date, code'),
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
next_trading_session as
    (
        (
            select distinct on (
                exchange_name
                ) exchange_name,
                  null as country_name,
                  date
            from {{ ref('exchange_schedule') }}
            where open_at > now()
            order by exchange_name, date
        )
        union all
        (
            select distinct on (
                country_name
                ) null as exchange_name,
                  country_name,
                  date
            from {{ ref('exchange_schedule') }}
            where open_at > now()
            order by country_name, date
        )
    ),
prices_with_split_rates as
    (
        SELECT eod_historical_prices.code,
               eod_historical_prices.date,
               case when close > 0 then adjusted_close / close end as split_rate,
               polygon_stock_splits.split_to::numeric /
               polygon_stock_splits.split_from::numeric            as split_rate_next_day,
               eod_historical_prices.open,
               eod_historical_prices.high,
               eod_historical_prices.low,
               eod_historical_prices.close,
               eod_historical_prices.adjusted_close,
               eod_historical_prices.volume
        from {{ source('eod', 'eod_historical_prices') }}
                 join {{ ref('base_tickers') }} on base_tickers.symbol = eod_historical_prices.code
                 left join next_trading_session
                           on (next_trading_session.exchange_name = base_tickers.exchange_canonical or
                               (base_tickers.exchange_canonical is null and
                                next_trading_session.country_name = base_tickers.country_name))
                 left join {{ source('polygon', 'polygon_stock_splits') }}
                           on polygon_stock_splits.execution_date::date = next_trading_session.date
                               and polygon_stock_splits.ticker = eod_historical_prices.code
        order by code, date desc
    ),
prev_split as
    (
        select distinct on (code) code, date, split_rate, next_split_rate
        from (
                 select *,
                        lag(split_rate) over (partition by code order by date desc) as next_split_rate
                 from prices_with_split_rates
             ) t
        where abs(split_rate - next_split_rate) > 1e-6
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
           -- latest split period, so we ignore split_rate and use prev_split.next_split_rate
           when prices_with_split_rates.date > prev_split.date
               then coalesce(prev_split.next_split_rate, 1.0) * split_rate_next_day * close
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
