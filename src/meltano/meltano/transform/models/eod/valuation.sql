{{
  config(
    materialized = "table",
    dist = "symbol",
    post_hook=[
      pk('symbol'),
    ]
  )
}}

select code                                            as symbol,
       (valuation->>'ForwardPE')::numeric              as forward_pe,
--        (valuation->>'TrailingPE')::numeric             as trailing_pe,
--        (valuation->>'PriceBookMRQ')::numeric           as price_book_mrq,
       (valuation->>'PriceSalesTTM')::numeric          as price_sales_ttm,
--        (valuation->>'EnterpriseValue')::bigint         as enterprise_value,
       (valuation->>'EnterpriseValueEbitda')::numeric  as enterprise_value_ebidta,
       (valuation->>'EnterpriseValueRevenue')::numeric as enterprise_value_revenue,
       case
           when is_date(updatedat)
               then updatedat::timestamp
           else _sdc_batched_at
           end                                         as updated_at
from {{ source('eod', 'eod_fundamentals') }}
