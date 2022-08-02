{{
  config(
    materialized = "table",
    dist = "symbol",
    post_hook=[
      pk('symbol'),
    ]
  )
}}

select code                                                         as symbol,
       (valuation ->> 'ForwardPE') :: double precision              as forward_pe,
       (valuation ->> 'TrailingPE') :: double precision             as trailing_pe,
       (valuation ->> 'PriceBookMRQ') :: double precision           as price_book_mrq,
       (valuation ->> 'PriceSalesTTM') :: double precision          as price_sales_ttm,
       (valuation ->> 'EnterpriseValue') :: bigint                  as enterprise_value,
       (valuation ->> 'EnterpriseValueEbitda') :: double precision  as enterprise_value_ebidta,
       (valuation ->> 'EnterpriseValueRevenue') :: double precision as enterprise_value_revenue,
       updatedat::date                                              as updated_at
from {{ source('eod', 'eod_fundamentals') }}
inner join {{ ref('tickers') }} as t on eod_fundamentals.code = t.symbol
