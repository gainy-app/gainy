{{
  config(
    materialized = "table",
    post_hook=[
      index(this, 'symbol', true),
    ]
  )
}}

select distinct on (
    code
    ) code as symbol,
      date                                                                                                           as date_0d,
      adjusted_close                                                                                                 as price_0d,
      first_value(date)
      over (partition by code order by date desc range between interval '1 week' following and unbounded following)  as date_1w,
      first_value(adjusted_close)
      over (partition by code order by date desc range between interval '1 week' following and unbounded following)  as price_1w,
      first_value(date)
      over (partition by code order by date desc range between interval '10 days' following and unbounded following) as date_10d,
      first_value(adjusted_close)
      over (partition by code order by date desc range between interval '10 days' following and unbounded following) as price_10d,
      first_value(date)
      over (partition by code order by date desc range between interval '1 month' following and unbounded following) as date_1m,
      first_value(adjusted_close)
      over (partition by code order by date desc range between interval '1 month' following and unbounded following) as price_1m,
      first_value(date)
      over (partition by code order by date desc range between interval '3 month' following and unbounded following) as date_3m,
      first_value(adjusted_close)
      over (partition by code order by date desc range between interval '3 month' following and unbounded following) as price_3m,
      first_value(date)
      over (partition by code order by date desc range between interval '1 year' following and unbounded following)  as date_1y,
      first_value(adjusted_close)
      over (partition by code order by date desc range between interval '1 year' following and unbounded following)  as price_1y,
      first_value(date)
      over (partition by code order by date desc range between interval '5 year' following and unbounded following)  as date_5y,
      first_value(adjusted_close)
      over (partition by code order by date desc range between interval '5 year' following and unbounded following)  as price_5y,
      last_value(date)
      over (partition by code order by date desc rows between current row and unbounded following)                   as date_all,
      last_value(adjusted_close)
      over (partition by code order by date desc rows between current row and unbounded following)                   as price_all
from {{ ref('historical_prices') }}
order by code, date desc
