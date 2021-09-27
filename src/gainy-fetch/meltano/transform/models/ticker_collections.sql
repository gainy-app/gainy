{{
  config(
    materialized = "table",
    post_hook=[
      fk(this, 'collection_id', 'collections', 'id'),
      fk(this, 'symbol', 'tickers', 'symbol'),
      'create unique index if not exists {{ get_index_name(this, "symbol__collection_id") }} (symbol, collection_id)',
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
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '1 dollar stocks' where ct.price <=1.5 and ct.chrt <=1
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '10 dollars stocks' where ct.price >7 and ct.price <=13 and ct.chrt <=0.2
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '100 dollars stocks' where ct.price between 90 and 110 and ct.chrt <=0.2
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '2 dollars stocks' where ct.price >1.5 and ct.price <=3 and ct.chrt <=1
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '20 dollars stocks' where ct.price between 17 and 23 and ct.chrt <=0.2
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '3D printing ETFs' where ct.ttype ='etf' and ct.g_industry ilike '%3d%print%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '3D printing stocks' where ct.g_industry ilike '%3d%print%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '5 dollars stock' where ct.price >3 and ct.price <=7 and ct.chrt <=0.5
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '5$ Biotech Stocks' where ct.price >3 and ct.price <=7 and ct.chrt <=0.5 and ct.g_industry ilike '%biotech%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '50 dollars stocks' where ct.price between 45 and 55 and ct.chrt <=0.2
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '5G stocks' where ct.g_industry ilike '%5g%com%unicat%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Agriculture stocks' where ct.g_industry ilike '%agricul%tech%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'AI companies stocks' where ct.g_industry ilike '%analyt%ai%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Airline Stocks' where ct.g_industry ilike '%airlines%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Alcohol Stocks' where ct.g_industry ilike '%wine%liquor%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'AR, VR, XR Stocks' where ct.g_industry ilike '%ar_vr%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Automotive stocks' where ct.g_industry ilike '%electr%vehic%' or ct.g_industry ilike '%auto%deal%' or ct.g_industry ilike '%auto%manufact%' or ct.g_industry ilike '%auto%marketpl%' or ct.g_industry ilike '%auto%part%servic%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Banking ETFs' where ct.ttype ='etf' and ct.g_industry ilike '%bank%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Biotech ETFs' where ct.ttype ='etf' and ct.g_industry ilike '%biotech%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Biotech Stocks' where ct.g_industry ilike '%biotech%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Cannabis ETFs' where ct.ttype ='etf' and ct.g_industry ilike '%cannabis%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Cannabis Penny stocks' where ct.investcat ='penny' and ct.price <=5 and ct.g_industry ilike '%cannabis%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Cannabis stocks' where ct.g_industry ilike '%cannabis%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Cars stocks' where ct.g_industry ilike '%electr%vehic%' or ct.g_industry ilike '%auto%deal%' or ct.g_industry ilike '%auto%manufact%' or ct.g_industry ilike '%auto%marketpl%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Casino stocks' where ct.g_industry ilike '%casin%resort%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Chinese stocks' where ct.country_name ilike '%china%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Cloud computing stocks' where ct.g_industry ilike '%enterpric%cloud%' or ct.g_industry ilike '%tech%conglomer%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Coffee stocks' where ct.g_industry ilike '%cof%e%tea%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Commodities stocks' where ct.g_industry ilike '%agricult%tech%' or ct.g_industry ilike '%bas%chemi%' or ct.g_industry ilike '%coal%' or ct.g_industry ilike '%energ%explor%' or ct.g_industry ilike '%industr%metal%' or ct.g_industry ilike '%liqui%natur%gas%' or ct.g_industry ilike '%mlp%' or ct.g_industry ilike '%oil%gas%produc%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Concrete stocks' where ticker_code in ('smid','uscr')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Construction stocks' where ct.g_industry ilike '%home%construct%maint%' or ct.g_industry ilike '%construct%infrastruct%' or ct.g_industry ilike '%build%mater%' or ct.g_industry ilike '%build%produc%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Consumer staples ETFs' where ct.ttype ='etf' and lower(ct.gics_sector) ilike '%consum%stapl%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Consumer staples stocks' where lower(ct.gics_sector) ilike '%consum%stapl%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Copper ETFs' where ct.ttype ='etf' and ct.g_industry ilike '%industr%metal%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Copper stocks' where ct.g_industry ilike '%industr%metal%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Cruise line ETFs' where ct.ttype ='etf' and (ct.g_industry ilike '%onlin%travel%compan%' or ct.g_industry ilike '%maritime%transport%')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Cruise line stocks' where ct.g_industry ilike '%onlin%travel%compan%' or ct.g_industry ilike '%maritime%transport%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Data stocks' where ct.g_industry ilike '%analyt%ai%' or ct.g_industry ilike '%data%center%' and ct.g_industry not ilike '%reit%' or ct.g_industry ilike '%data%stor%manag%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Defense stocks' where ct.g_industry ilike '%aerospac%defen%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Development companies stocks' where ct.investcat ='defensive'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Discretionary consumer stocks' where lower(ct.gics_sector) ilike '%consum%discretionar%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Dividend Gold stocks' where ct.g_industry ilike '%gold%silver%' and lower(ct.investcat) ='dividend'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Drone stock companies' where ct.g_industry ilike '%drone%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'E-commerce and Retail stocks' where ct.g_industry ilike '%e-commerc%platform%' or ct.g_industry ilike '%e-commerc%operat%' or ct.g_industry ilike '%apparel%manufact%retail%' or ct.g_industry ilike '%discount%retail%' or ct.g_industry ilike '%home%furnit%retail%' or ct.g_industry ilike '%luxur%retail%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Education stocks' where ct.g_industry ilike '%educat%servic%' or ct.g_industry ilike '%educat%tech%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Electric vehicles stocks' where ct.g_industry ilike '%electr%vehicl%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Energy company ETFs' where ct.ttype ='etf' and (ct.g_industry ilike '%clean%energ%' or ct.g_industry ilike '%energ%explor%' or ct.g_industry ilike '%renew%energ%' or ct.g_industry ilike '%solar%energ%' or ct.g_industry ilike '%oil%gas%produc%' or ct.g_industry ilike '%liqui%natur%gas%' or ct.g_industry ilike '%electr%generat%' or ct.g_industry ilike '%power%gener%distrib%' or ct.g_industry ilike '%oil%gas%servic%' or ct.g_industry ilike '%electron%power%management%')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Energy company stocks' where ct.g_industry ilike '%clean%energ%' or ct.g_industry ilike '%energ%explor%' or ct.g_industry ilike '%renew%energ%' or ct.g_industry ilike '%solar%energ%' or ct.g_industry ilike '%oil%gas%produc%' or ct.g_industry ilike '%liqui%natur%gas%' or ct.g_industry ilike '%electr%generat%' or ct.g_industry ilike '%power%gener%distrib%' or ct.g_industry ilike '%oil%gas%servic%' or ct.g_industry ilike '%electron%power%management%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Energy Penny stocks' where ct.investcat ='penny' and ct.price <=5 and (ct.g_industry ilike '%clean%energ%' or ct.g_industry ilike '%energ%explor%' or ct.g_industry ilike '%renew%energ%' or ct.g_industry ilike '%solar%energ%' or ct.g_industry ilike '%oil%gas%produc%' or ct.g_industry ilike '%liqui%natur%gas%' or ct.g_industry ilike '%electr%generat%' or ct.g_industry ilike '%power%gener%distrib%' or ct.g_industry ilike '%oil%gas%servic%' or ct.g_industry ilike '%electron%power%management%')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Entertainment stocks' where ct.g_industry ilike '%tv%radio%' or ct.g_industry ilike '%streaming%' or ct.g_industry ilike '%gaming%esport%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Esport stocks' where ct.ticker_code in ('has', 'gmbl', 'slgg', 'inse', 'aese', 'ea', 'atvi', 'ttwo', 'dkng')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'European stocks' where ct.country_group ilike '%europe%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'EV batteries stocks' where ct.g_industry ilike '%batter%tech%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'FAANG (big tech) stocks' where ct.g_industry ilike '%tech%conglomerat%' or ct.ticker_code in ('fb', 'amzn', 'aapl', 'nflx', 'goog', 'googl')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Farming stocks' where ct.g_industry ilike '%agricultur%tech%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Fast food stocks' where ct.g_industry ilike '%casual%din%n%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Finance stocks' where lower(ct.gics_sector) ilike '%financ%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Fintech stocks' where ct.g_industry ilike '%financ%exchang%data%' or ct.g_industry ilike '%fintech%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Food Delivery stocks' where ct.g_industry ilike '%food%deliver%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Food stocks' where ct.g_industry ilike '%bread%condiment%' or ct.g_industry ilike '%casual%din%n%' or ct.g_industry ilike '%dairy%' or ct.g_industry ilike '%fruit%vegetabl%' or ct.g_industry ilike '%meat%poultry%' or ct.g_industry ilike '%snack%cand%' or ct.g_industry ilike '%soft%sport%drink%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Fuel stocks' where ct.g_industry ilike '%oil%gas%produc%' or ct.g_industry ilike '%liqui%natur%gas%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Furniture and decor stocks' where ct.g_industry ilike '%home%furnitur%retail%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Gambling stocks' where ct.g_industry ilike '%casin%resort%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Gaming companies stocks' where ct.g_industry ilike '%gam%esport%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Gold companies stocks' where ct.g_industry ilike '%gold%silver%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Green energy stocks' where ct.g_industry ilike '%clean%energ%' or ct.g_industry ilike '%energ%explor%' or ct.g_industry ilike '%renew%energ%' or ct.g_industry ilike '%solar%energ%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Gun stocks' where ct.g_industry ilike '%secur%protect%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Health insurance stocks' where ct.g_industry ilike '%health%insuranc%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Healthcare ETFs' where ct.ttype ='etf' and lower(ct.gics_sector) ilike '%health%car%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Healthcare Stocks' where lower(ct.gics_sector) ilike '%health%car%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Heavy Equipment Stocks' where ct.g_industry ilike '%construct%machine%' or ct.g_industry ilike '%heavy%machine%part%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Hotel stocks' where (ct.g_industry ilike '%hotel%' and ct.g_industry not ilike '%reit%') or ct.g_industry ilike '%casin%resort%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Information security stocks' where ct.g_industry ilike '%cyber%secur%' or ct.g_industry ilike '%ident%authenticat%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Infrastructure stocks' where ct.g_industry ilike '%construct%infrastruct%' or ct.g_industry ilike '%telecom%infrastruct%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Insurance stocks' where ct.g_industry ilike '%health%insuranc%' or ct.g_industry ilike '%insuranc%broker%' or ct.g_industry ilike '%insuranc%marketplac%tech%' or ct.g_industry ilike '%mortgag%insuranc%' or ct.g_industry ilike '%multiline%insuranc%' or ct.g_industry ilike '%property%casualt%insuranc%' or ct.g_industry ilike '%life%insuranc%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'International stocks' where ct.country_name not ilike '%united%states%' and ct.country_name not ilike '%usa%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Internet of things stocks' where ct.g_industry ilike 'iot%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Latam Airlines Stocks' where ct.g_industry ilike '%airline%' and ct.country_group ilike '%latam%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Latam stocks' where ct.country_group ilike '%latam%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Lithium stocks' where ct.ticker_code in ('ulbi', 'flux', 'tanh', 'pll', 'sedg')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Meat stocks' where ct.g_industry ilike '%meat%poultry%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Medical supplies company stocks' where ct.g_industry ilike '%health%care%service%equipment%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Mining companies stocks' where ct.g_industry ilike '%gold%silver%' or ct.g_industry ilike '%industr%metal%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'MLP to invest' where ct.g_industry ilike '%mlp%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Nanotechnology stocks' where ct.g_industry ilike '%nanotech%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Natural gas stocks' where ct.g_industry ilike '%oil%gas%produc%' or ct.g_industry ilike '%liqui%natur%gas%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Nuclear energy stocks' where ct.ticker_code in ('lbtr','aep','exc','xel','pesi','holi','gvp')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Oil ETF' where ct.ttype ='etf' and (ct.g_industry ilike '%oil%gas%produc%' or ct.g_industry ilike '%oil%gas%servic%')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Oil stocks' where ct.g_industry ilike '%oil%gas%produc%' or ct.g_industry ilike '%oil%gas%servic%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Penny stocks' where ct.investcat ='penny' and ct.price <=1.5 and ct.chrt <=0.2
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Pharmaceutical stocks' where ct.g_industry ilike '%pharm%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Precious metals stocks' where ct.g_industry ilike '%gold%silver%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Real estate stocks' where lower(ct.gics_sector) ilike '%real%estat%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Recent IPOs' where ct.ipo_date > (current_date - interval '6 month')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Robotics company stocks' where ct.g_industry ilike '%analytic%ai%' or ct.g_industry ilike '%comput%visi%sens%' or ct.g_industry ilike 'iot%' or ct.g_industry ilike '%precis%manufact%' or ct.g_industry ilike '%health%car%services%equipment%robot%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Self driving car stocks' where ct.ticker_code in ('lazr', 'f', 'aptv', 'tsla', 'googl', 'xpev', 'amba', 'qcom', 'nvda')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Semiconductors stocks' where ct.g_industry ilike '%semiconduct%equip%' or ct.g_industry ilike '%semiconduct%manufact%' or ct.g_industry ilike '%semiconduct%material%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Shipping stocks' where ct.g_industry ilike '%maritim%transport%' or ct.g_industry ilike '%truck%' or ct.g_industry ilike '%air%carg%transport%' or ct.g_industry ilike '%rail%freight%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Ships stocks' where ct.g_industry ilike '%maritim%transport%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Silver companies stocks' where ct.g_industry ilike '%gold%silver%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Silver ETFs' where ct.ttype ='etf' and ct.g_industry ilike '%gold%silver%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Space stocks' where ct.g_industry ilike '%aerospac%defen%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Steel stocks' where ct.ticker_code in ('zeus', 'schn', 'stld', 'usap', 'zkin')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Stocks in sports' where ct.g_industry ilike '%prosport%manag%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Technology stocks' where lower(ct.gics_sector) ilike '%technolog%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Telecom stocks' where ct.g_industry ilike '%5g%com%unicat%' or ct.g_industry ilike '%internet%cellular%provid%' or ct.g_industry ilike '%telecom%infrastruct%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Toilet paper companies stocks' where ct.ticker_code in ('wmt', 'pg', 'kmb', 'itp', 'clw', 'cost', 'kr')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Transportation stocks' where ct.g_industry ilike '%maritim%transport%' or ct.g_industry ilike '%airline%' or ct.g_industry ilike '%air%carg%transport%' or ct.g_industry ilike '%rail%freight%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Travel stocks' where ct.g_industry ilike '%online%travel%compan%' or ct.g_industry ilike '%airline%' or ct.g_industry ilike '%casin%resort%' or ct.g_industry ilike '%hotel%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Trucking stocks' where ct.g_industry ilike '%truck%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'US Food stocks' where (ct.g_industry ilike '%bread%condiment%' or ct.g_industry ilike '%casual%din%' or ct.g_industry ilike '%dairy%' or ct.g_industry ilike '%fruit%vegetabl%' or ct.g_industry ilike '%meat%poultr%' or ct.g_industry ilike '%snack%cand%' or ct.g_industry ilike '%soft%sport%drink%') and (ct.country_name ilike '%united states%' or ct.country_name ilike '%usa%')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'USA Technology stocks' where lower(ct.gics_sector) ilike '%technolog%' and (ct.country_name ilike '%united states%' or ct.country_name ilike '%usa%')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Utilities ETFs' where ct.ttype ='etf' and lower(ct.gics_sector) ilike '%utilities%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Utilities stocks' where lower(ct.gics_sector) ilike '%utilities%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Waste management stocks' where ct.g_industry ilike '%wast%manag%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Water companies stocks' where ct.g_industry ilike '%water%manag%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Wine stocks' where ct.g_industry ilike '%wine%liquor%'
         )
SELECT t2.symbol, collection_id
from tmp_ticker_collections
    join {{ ref ('tickers') }} t2 on tmp_ticker_collections.symbol = t2.symbol
    join {{ ref ('collections') }} c2 on tmp_ticker_collections.collection_id = c2.id
