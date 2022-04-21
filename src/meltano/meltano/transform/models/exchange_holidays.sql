{{
  config(
    materialized = "table",
    post_hook=[
      'create unique index if not exists "date__exchange_name" ON {{ this }} (date, exchange_name)',
      'create unique index if not exists "date__country_name" ON {{ this }} (date, country_name)',
    ]
  )
}}

select date,
       exchange_canonical as exchange_name,
       country_name
from (
         select exchange_canonical, null as country_name, date, prices_count,
                avg(prices_count)
                over (partition by exchange_canonical order by date range between interval '1 week' preceding and interval '1 day' preceding) as avg_prices_count
         from (
                  SELECT exchange_canonical, date, count(*) as prices_count
                  FROM {{ ref('historical_prices') }}
                           join {{ ref('base_tickers') }} on base_tickers.symbol = historical_prices.code
                  where date >= now() - interval '1 year' - interval '1 week'
                    and exchange_canonical is not null
                  group by exchange_canonical, date
              ) t
         union all
         select null, country_name, date, prices_count,
                avg(prices_count)
                over (partition by country_name order by date range between interval '1 week' preceding and interval '1 day' preceding) as avg_prices_count
         from (
                  SELECT country_name, date, count(*) as prices_count
                  FROM {{ ref('historical_prices') }}
                           join {{ ref('base_tickers') }} on base_tickers.symbol = historical_prices.code
                  where date >= now() - interval '1 year' - interval '1 week'
                    and exchange_canonical is null
                    and country_name is not null
                  group by country_name, date
              ) t
     ) t
where prices_count / avg_prices_count < 0.5
