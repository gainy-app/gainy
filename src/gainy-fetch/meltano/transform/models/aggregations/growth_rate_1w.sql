{{
  config(
    materialized = "view"
  )
}}

select DISTINCT ON (
    symbol,
    date_part('year', date::date),
    date_part('week', date::date)
    ) symbol                                                                                                            as symbol,
      date::timestamp,
      first_value(growth_rate_1w)
      OVER (partition by symbol, date_part('year', date::date), date_part('week', date::date) ORDER BY date::date desc) as growth_rate
from {{ ref('historical_growth_rate') }}
order by symbol, date_part('year', date::date), date_part('week', date::date), date