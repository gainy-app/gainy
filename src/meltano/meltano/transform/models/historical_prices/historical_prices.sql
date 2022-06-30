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


-- Execution Time: 61540.802 ms
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
    )
SELECT code,
       (code || '_' || date)::varchar            as id,
       substr(date, 0, 5)                        as date_year,
       (substr(date, 0, 8) || '-01')::timestamp  as date_month,
       date_trunc('week', date::date)::timestamp as date_week,
       adjusted_close,
       close,
       date::date,
       high,
       low,
       open,
       volume
from {{ source('eod', 'eod_historical_prices') }}
{% if is_incremental() %}
    left join max_updated_at using (code)
    where _sdc_batched_at >= max_updated_at.max_date or max_updated_at.max_date is null
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
