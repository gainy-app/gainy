{{
  config(
    materialized = "view"
  )
}}

select DISTINCT ON (
    industry_id,
    date_part('year', date::date),
    date_part('week', date::date)
    ) industry_id,
      date,
      first_value(median_price)
      OVER (partition by industry_id, date_part('year', date::date), date_part('week', date::date) ORDER BY date::date desc) as median_price
from {{ ref('industry_stats_daily') }}
order by industry_id, date_part('year', date::date), date_part('week', date::date), date