{{
  config(
    materialized = "table",
    dist = "symbol",
    post_hook=[
      index(this, 'symbol', true),
    ]
  )
}}

select code                                                       as symbol,
       (valuation ->> 'ForwardPE') :: double precision              as forward_pe,
       (valuation ->> 'TrailingPE') :: double precision             as trailing_pe,
       (valuation ->> 'PriceBookMRQ') :: double precision           as price_book_mrq,
       (valuation ->> 'PriceSalesTTM') :: double precision          as price_sales_ttm,
       (valuation ->> 'EnterpriseValue') :: bigint                  as enterprise_value,
       (valuation ->> 'EnterpriseValueEbitda') :: double precision  as enterprise_value_ebidta,
       (valuation ->> 'EnterpriseValueRevenue') :: double precision as enterprise_value_revenue
from {{ source('eod', 'eod_fundamentals') }} f inner join {{  ref('tickers') }} as t on f.code = t.symbol


