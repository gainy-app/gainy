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
         with countries as
                  (
                      select distinct country_name from {{ source('gainy', 'exchanges') }}
                  )

         select exchanges.name    as exchange_canonical,
                null              as country_name,
                date_series::date as date,
                prices_count      as prices_count,
                avg(prices_count)
                over (
                    partition by exchange_canonical
                    order by date
                    range between interval '1 week' preceding and interval '1 day' preceding
                    )             as avg_prices_count
         FROM generate_series(now() - interval '1 year' - interval '1 week', now(), '1 day') date_series
                  join {{ source('gainy', 'exchanges') }} on true
                  left join (
                                SELECT exchange_canonical, date, count(*) as prices_count
                                FROM {{ ref('historical_prices') }}
                                         join {{ ref('base_tickers') }} using (symbol)
                                where date >= now() - interval '1 year' - interval '1 week'
                                  and exchange_canonical is not null
                                group by exchange_canonical, date
                            ) t on t.date = date_series::date and t.exchange_canonical = exchanges.name

         union all

         select null              as exchange_canonical,
                countries.country_name,
                date_series::date as date,
                prices_count      as prices_count,
                avg(prices_count)
                over (
                    partition by countries.country_name
                    order by date
                    range between interval '1 week' preceding and interval '1 day' preceding
                    )             as avg_prices_count
         FROM generate_series(now() - interval '1 year' - interval '1 week', now(), '1 day') date_series
                  join countries on true
                  left join (
                                SELECT country_name, date, count(*) as prices_count
                                FROM {{ ref('historical_prices') }}
                                         join {{ ref('base_tickers') }} using (symbol)
                                where date >= now() - interval '1 year' - interval '1 week'
                                  and exchange_canonical is null
                                  and country_name is not null
                                group by country_name, date
                            ) t on t.date = date_series::date and t.country_name = countries.country_name
     ) t
where prices_count / avg_prices_count < 0.5
   or prices_count is null
