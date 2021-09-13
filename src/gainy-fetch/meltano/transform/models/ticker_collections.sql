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
                    t.country_name,
                    t.ipo_date,
                    latest_price.close                                           as price,
                    (latest_price.close - latest_price.open) / latest_price.open as chrt,
                    t.type                                                       as ttype,
                    gi.name                                                      as g_industry,
                    t.gic_sector                                                 as gics_sector,
                    lower(c.name)                                                as investcat,
                    CASE
                        WHEN countries.region = 'Europe' THEN 'europe'
                        WHEN countries."sub-region" LIKE '%Latin America%' THEN 'latam'
                        END                                                      as country_group
             from {{ ref('tickers') }} t
                      JOIN price latest_price ON latest_price.code = t.symbol AND latest_price.inv_row_number = 1
                      JOIN {{ ref('ticker_industries') }} ti on t.symbol = ti.symbol
                      JOIN {{ ref('gainy_industries') }} gi on ti.industry_id = gi.id
                      JOIN {{ ref('ticker_categories') }} tc on t.symbol = tc.symbol
                      JOIN {{ ref('categories') }} c on tc.category_id = c.id
                      JOIN raw_countries countries
                           on countries.name = t.country_name OR countries."alpha-2" = t.country_name OR
                              countries."alpha-3" = t.country_name
         ),
     tmp_ticker_collections as
         (
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '1 dollar stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '10 dollars stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '100 dollars stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '2 dollars stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '20 dollars stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '3D printing ETFs' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '3D printing stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '5 dollars stock' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '5$ Biotech Stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '50 dollars stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '5G stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Agriculture stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'AI companies stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Airline Stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Alcohol Stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'AR, VR, XR Stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Automotive stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Banking ETFs' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Biotech ETFs' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Biotech Stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Cannabis ETFs' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Cannabis Penny stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Cannabis stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Cars stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Casino stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Chinese stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Cloud computing stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Coffee stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Commodities stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Concrete stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Construction stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Consumer staples ETFs' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Consumer staples stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Copper ETFs' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Copper stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Cruise line ETFs' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Cruise line stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Data stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Defense stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Development companies stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Discretionary consumer stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Dividend Gold stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Drone stock companies' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'E-commerce and Retail stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Education stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Electric vehicles stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Energy company ETFs' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Energy company stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Energy Penny stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Entertainment stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Esport stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'European stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'EV batteries stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'FAANG (big tech) stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Farming stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Fast food stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Finance stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Fintech stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Food Delivery stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Food stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Fuel stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Furniture and decor stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Gambling stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Gaming companies stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Gold companies stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Green energy stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Gun stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Health insurance stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Healthcare ETFs' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Healthcare Stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Heavy Equipment Stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Hotel stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Information security stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Infrastructure stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Insurance stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'International stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Internet of things stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Latam Airlines Stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Latam stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Lithium stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Meat stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Medical supplies company stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Mining companies stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'MLP to invest' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Nanotechnology stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Natural gas stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Nuclear energy stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Oil ETF' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Oil stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Penny stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Pharmaceutical stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Precious metals stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Real estate stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Recent IPOs' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Robotics company stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Self driving car stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Semiconductors stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Shipping stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Ships stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Silver companies stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Silver ETFs' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Space stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Steel stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Stocks in sports' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Technology stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Telecom stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Toilet paper companies stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Transportation stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Travel stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Trucking stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'US Food stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'USA Technology stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Utilities ETFs' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Utilities stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Waste management stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Water companies stocks' where 
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Wine stocks' where 
         )
SELECT t2.symbol, collection_id
from tmp_ticker_collections
    join {{ ref ('tickers') }} t2 on tmp_ticker_collections.symbol = t2.symbol
    join {{ ref ('collections') }} c2 on tmp_ticker_collections.collection_id = c2.id
