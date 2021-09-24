{{
  config(
    materialized = "view"
  )
}}

select DISTINCT ON (
    code,
    date_part('year', date::date),
    date_part('month', date::date)
    ) code                                                                                                            as symbol,
      date::timestamp,
      open,
      first_value(close)
      OVER (partition by code, date_part('year', date::date), date_part('month', date::date) ORDER BY date::date desc) as close,
      first_value(adjusted_close)
      OVER (partition by code, date_part('year', date::date), date_part('month', date::date) ORDER BY date::date desc) as adjusted_close
from historical_prices
order by code, date_part('year', date::date), date_part('month', date::date), date