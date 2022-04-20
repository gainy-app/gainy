{{
  config(
    materialized = "table",
    post_hook=[
      'create unique index if not exists "date__exchange_name" ON {{ this }} (date, exchange_name)',
      'create unique index if not exists "date__country_name" ON {{ this }} (date, country_name)',
    ]
  )
}}

with exchange_holidays as
         (
             select date,
                    exchange_canonical as exchange_name,
                    country_name
             from (
                      select *,
                             avg(prices_count)
                             over (partition by exchange_canonical order by date range between interval '1 week' preceding and interval '1 day' preceding) as avg_prices_count
                      from (
                               SELECT exchange_canonical, date, count(*) as prices_count
                               FROM {{ ref('historical_prices') }}
                                        join {{ ref('base_tickers') }} on base_tickers.symbol = historical_prices.code
                               where date >= now() - interval '1 year' - interval '1 week'
                               group by exchange_canonical, date
                           ) t
                  ) t
                      join {{ source('gainy', 'exchanges') }} on exchange_canonical = exchanges.name
             where prices_count / avg_prices_count < 0.1
         )

select (date || '_' || exchange_name)::varchar as id,
       date,
       exchange_name,
       null::varchar                          as country_name
from exchange_holidays

union all

select distinct on (
    date, country_name
    ) (date || '_' || country_name)::varchar as id,
      date,
      null::varchar                          as exchange_name,
      country_name
from exchange_holidays
