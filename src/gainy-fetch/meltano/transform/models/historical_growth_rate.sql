{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    incremental_strategy='insert_overwrite',
    post_hook=[
      index(this, 'symbol'),
      index(this, 'date'),
    ]
  )
}}

{% if is_incremental() %}
with
     max_updated_at as (select max(date) as max_date from {{ this }})
{% endif %}
SELECT code as symbol,
       date ::date, CASE
    WHEN first_value(date ::date) OVER (partition by code ORDER BY date ::date ROWS 1 PRECEDING) <=
         date ::date - interval '1 day'
                AND first_value(adjusted_close) OVER (partition by code ORDER BY date::date ROWS 1 PRECEDING) > 0
               THEN adjusted_close /
                    first_value(adjusted_close) OVER (partition by code ORDER BY date::date ROWS 1 PRECEDING)
           END as growth_rate_1d,

       CASE
           WHEN first_value(date::date)
                OVER (partition by code ORDER BY date::date RANGE INTERVAL '1 week' PRECEDING) <=
                date::date - interval '1 week' + interval '3 days'
                AND first_value(adjusted_close)
                                     OVER (partition by code ORDER BY date::date RANGE INTERVAL '1 week' PRECEDING) > 0
               THEN adjusted_close / first_value(adjusted_close)
                                     OVER (partition by code ORDER BY date::date RANGE INTERVAL '1 week' PRECEDING)
           END as growth_rate_1w,

       CASE
           WHEN first_value(date::date)
                OVER (partition by code ORDER BY date::date RANGE INTERVAL '1 month' PRECEDING) <=
                date::date - interval '1 month' + interval '4 days'
                AND first_value(adjusted_close)
                                     OVER (partition by code ORDER BY date::date RANGE INTERVAL '1 month' PRECEDING) > 0
               THEN adjusted_close / first_value(adjusted_close)
                                     OVER (partition by code ORDER BY date::date RANGE INTERVAL '1 month' PRECEDING)
           END as growth_rate_1m,

       CASE
           WHEN first_value(date::date)
                OVER (partition by code ORDER BY date::date RANGE INTERVAL '3 month' PRECEDING) <=
                date::date - interval '3 month' + interval '4 days'
                AND first_value(adjusted_close)
                                     OVER (partition by code ORDER BY date::date RANGE INTERVAL '3 month' PRECEDING) > 0
               THEN adjusted_close / first_value(adjusted_close)
                                     OVER (partition by code ORDER BY date::date RANGE INTERVAL '3 month' PRECEDING)
           END as growth_rate_3m,

       CASE
           WHEN first_value(date::date)
                OVER (partition by code ORDER BY date::date RANGE INTERVAL '1 year' PRECEDING) <=
                date::date - interval '1 year' + interval '4 days'
                AND first_value(adjusted_close)
                                     OVER (partition by code ORDER BY date::date RANGE INTERVAL '1 year' PRECEDING) > 0
               THEN adjusted_close / first_value(adjusted_close)
                                     OVER (partition by code ORDER BY date::date RANGE INTERVAL '1 year' PRECEDING)
           END as growth_rate_1y
from {{ ref('historical_prices') }}
join {{ ref('tickers') }} ON tickers.symbol = historical_prices.code
{% if is_incremental() %}
    join max_updated_at on true
    where date::date >= max_updated_at.max_date
{% endif %}