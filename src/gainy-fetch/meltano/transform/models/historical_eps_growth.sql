{{
  config(
    materialized = "table",
    post_hook=[
      index(this, 'symbol'),
      index(this, 'updated_at'),
    ]
  )
}}

SELECT eh0.symbol,
       eh0.date::date as updated_at,
       CASE WHEN ABS(eh1.eps_actual) > 0 THEN cbrt(eh0.eps_actual / eh1.eps_actual) - 1 END as value
from {{ ref('earnings_history') }} eh0
         LEFT JOIN {{ ref('earnings_history') }} eh0_next
                   ON eh0_next.symbol = eh0.symbol AND
                      eh0_next.date::timestamp > eh0.date::timestamp AND
                      eh0_next.date::timestamp < NOW()
         JOIN {{ ref('earnings_history') }} eh1
              ON eh1.symbol = eh0.symbol AND
                 eh1.date::timestamp < NOW() - interval '3 years'
         LEFT JOIN {{ ref('earnings_history') }} eh1_next ON eh1_next.symbol = eh0.symbol AND
                                                eh1_next.date::timestamp <
                                                NOW() - interval '3 years' AND
                                                eh1_next.date::timestamp > eh1.date::timestamp
WHERE eh0.date::timestamp < NOW()
  AND eh0_next.symbol IS NULL
  AND eh1_next.symbol IS NULL