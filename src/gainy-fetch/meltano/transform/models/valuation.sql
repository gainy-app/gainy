{{
  config(
    materialized = "table",
    dist = "symbol",
    post_hook=[
      index(this, 'symbol', true),
      fk(this, 'symbol', 'tickers', 'symbol')
    ]
  )
}}

select code                                   as symbol,
       valuation ->> 'ForwardPE'              as forward_pe,
       valuation ->> 'TrailingPE'             as trailing_pe,
       valuation ->> 'PriceBookMRQ'           as price_book_mrq,
       valuation ->> 'PriceSalesTTM'          as price_sales_ttm,
       valuation ->> 'EnterpriseValue'        as enterprise_value,
       valuation ->> 'EnterpriseValueEbitda'  as enterprise_value_ebidta,
       valuation ->> 'EnterpriseValueRevenue' as enterprise_value_revenue
from fundamentals f inner join {{  ref('tickers') }} as t on f.code = t.symbol


