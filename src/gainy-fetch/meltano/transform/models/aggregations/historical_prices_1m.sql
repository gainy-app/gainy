{{
  config(
    materialized = "view"
  )
}}

select DISTINCT ON (
    code,
    date_part('year', date),
    date_part('month', date)
    ) code                                                                                                            as symbol,
      date::date,
      open,
      first_value(close)
      OVER (partition by code, date_part('year', date), date_part('month', date) ORDER BY date desc) as close,
      first_value(adjusted_close)
      OVER (partition by code, date_part('year', date), date_part('month', date) ORDER BY date desc) as adjusted_close
from {{ ref('historical_prices') }}
order by code, date_part('year', date), date_part('month', date), date