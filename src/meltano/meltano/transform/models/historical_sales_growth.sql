{{
  config(
    materialized = "table",
    post_hook=[
      pk('symbol'),
    ]
  )
}}

SELECT fisq0.symbol,
       fisq0.date::timestamp                                                                               as updated_at,
       CASE WHEN ABS(fisq1.total_revenue) > 0 THEN cbrt(fisq0.total_revenue / fisq1.total_revenue) - 1 END as total_revenue_growth_3y_yoy
from {{ ref('financials_income_statement_quarterly') }} fisq0
         LEFT JOIN {{ ref('financials_income_statement_quarterly') }} fisq0_next
                   ON fisq0_next.symbol = fisq0.symbol AND
                      fisq0_next.date::timestamp > fisq0.date::timestamp AND
                      fisq0_next.date::timestamp < NOW()
         JOIN {{ ref('financials_income_statement_quarterly') }} fisq1
              ON fisq1.symbol = fisq0.symbol AND
                 fisq1.date::timestamp < NOW() - interval '3 years'
         LEFT JOIN {{ ref('financials_income_statement_quarterly') }} fisq1_next
                   ON fisq1_next.symbol = fisq0.symbol AND
                      fisq1_next.date::timestamp < NOW() - interval '3 years' AND
                      fisq1_next.date::timestamp > fisq1.date::timestamp
WHERE fisq0.date::timestamp < NOW()
  AND fisq0_next.symbol IS NULL
  AND fisq1_next.symbol IS NULL