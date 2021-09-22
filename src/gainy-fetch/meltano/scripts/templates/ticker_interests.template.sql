{{
  config(
    materialized = "table",
    dist = "symbol",
    indexes=[
    ],
    post_hook=[
      fk(this, 'interest_id', 'interests', 'id'),
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
                      LEFT JOIN price latest_price ON latest_price.code = t.symbol AND latest_price.inv_row_number = 1
                      LEFT JOIN {{ ref('ticker_industries') }} ti on t.symbol = ti.symbol
                      LEFT JOIN {{ ref('gainy_industries') }} gi on ti.industry_id = gi.id
                      LEFT JOIN {{ ref('ticker_categories') }} tc on t.symbol = tc.symbol --here we have N:N relationship, so for interests we must use distinct in the end (we will get duplicates otherwise)
                      LEFT JOIN {{ ref('categories') }} c on tc.category_id = c.id
                      LEFT JOIN raw_countries countries
                           on countries.name = t.country_name OR countries."alpha-2" = t.country_name OR
                              countries."alpha-3" = t.country_name
         ),
     tmp_ticker_interests as
         (
-- __SELECT__ --
         )
SELECT distinct t2.symbol, interest_id
from tmp_ticker_interests
    join {{ ref ('tickers') }} t2 on tmp_ticker_interests.symbol = t2.symbol
    join {{ ref ('interests') }} c2 on tmp_ticker_interests.interest_id = c2.id
