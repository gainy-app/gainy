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
                    ti.industry_name                                             as g_industry,
                    t.gic_sector                                                 as gics_sector,
                    lower(c.name)                                                as investcat
             from tickers t
                      JOIN price latest_price ON latest_price.code = t.symbol AND latest_price.inv_row_number = 1
                      JOIN ticker_industries ti on t.symbol = ti.symbol
                      JOIN ticker_categories tc on t.symbol = tc.symbol
                      JOIN categories c on tc.category_id = c.id
         ),
     ticker_collections as
         (
-- __SELECT__ --
         )
SELECT t2.symbol, collection_id
from ticker_collections
         join {{ ref ('tickers') }} t2
on ticker_collections.symbol = t2.symbol
    join collections c2 on ticker_collections.collection_id = c2.id
