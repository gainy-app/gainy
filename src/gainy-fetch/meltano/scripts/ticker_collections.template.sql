{{
  config(
    materialized = "table",
    dist = "symbol",
    indexes=[
    ],
    post_hook=[
      fk(this, 'collection_id', 'collections', 'id'),
      fk(this, 'symbol', 'tickers', 'symbol'),
    ]
  )
}}

/* THE MODEL FILE IS AUTO GENERATED DURING BUILD, YOU SHOULD NOT EDIT THE MODEL, EDIT THE TEMPLATE INSTEAD  */

with price AS
         (
             select hp.*,
                    ROW_NUMBER() OVER (PARTITION BY hp.code ORDER BY hp.date::timestamp DESC) as inv_row_number
             from historical_prices hp
         ),
     ct as
         (
             select t.symbol                                                     as ticker_code,
                    latest_price.close                                           as price,
                    (latest_price.close - latest_price.open) / latest_price.open as chrt,
                    t.type                                                       as ttype,
                    gi.name                                                      as g_industry,
                    t.gic_sector                                                 as gics_sector,
                    lower(c.name)                                                as investcat
             from {{ ref('tickers') }} t
                      JOIN price latest_price ON latest_price.code = t.symbol AND latest_price.inv_row_number = 1
                      JOIN {{ ref('ticker_industries') }} ti on t.symbol = ti.symbol
                      JOIN {{ ref('gainy_industries') }} gi on ti.industry_id = gi.id
                      JOIN {{ ref('ticker_categories') }} tc on t.symbol = tc.symbol
                      JOIN {{ ref('categories') }} c on tc.category_id = c.id
         ),
     tmp_ticker_collections as
         (
-- __SELECT__ --
         )
SELECT t2.symbol, collection_id
from tmp_ticker_collections
    join {{ ref ('tickers') }} t2 on tmp_ticker_collections.symbol = t2.symbol
    join {{ ref ('collections') }} c2 on tmp_ticker_collections.collection_id = c2.id
