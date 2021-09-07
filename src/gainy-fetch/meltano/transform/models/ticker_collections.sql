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
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '1 dollar stocks' where ct.price <=1.5 and ct.chrt <=1
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '10 dollar stock' where ct.price >7 and ct.price <=13 and ct.chrt <=0.2
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '100 dollars stocks' where ct.price between 90 and 110 and ct.chrt <=0.2
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '2 dollars stocks' where ct.price >1.5 and ct.price <=3 and ct.chrt <=1
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '20 dollars stocks' where ct.price between 17 and 23 and ct.chrt <=0.2
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '3D printing ETFs' where ct.ttype ='etf' and lower(ct.g_industry) like '%3d%print%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '3D printing stocks' where lower(ct.g_industry) like '%3d%print%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '5 dollars stock' where ct.price >3 and ct.price <=7 and ct.chrt <=0.5
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '5$ Biotech Stocks ' where ct.price >3 and ct.price <=7 and ct.chrt <=0.5 and lower(ct.g_industry) like '%biotech%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '50 dollars stocks' where ct.price between 45 and 55 and ct.chrt <=0.2
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '5G stocks' where lower(ct.g_industry) like '%5g%com%unicat%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Agriculture stocks' where lower(ct.g_industry) like '%agricul%tech%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'AI companies stocks' where lower(ct.g_industry) like '%analyt%ai%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Airline Stocks' where lower(ct.g_industry) like '%airlines%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Alcohol Stocks' where lower(ct.g_industry) like '%wine%liquor%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'AR, VR, XR Stocks' where lower(ct.g_industry) like '%ar_vr%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Automotive stocks' where lower(ct.g_industry) like '%electr%vehic%' or lower(ct.g_industry) like '%auto%deal%' or lower(ct.g_industry) like '%auto%manufact%' or lower(ct.g_industry) like '%auto%marketpl%' or lower(ct.g_industry) like '%auto%part%servic%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Banking ETFs ' where ct.ttype ='etf' and lower(ct.g_industry) like '%bank%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Biotech ETFs' where ct.ttype ='etf' and lower(ct.g_industry) like '%biotech%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Biotech Stocks' where lower(ct.g_industry) like '%biotech%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Cannabis ETFs' where ct.ttype ='etf' and lower(ct.g_industry) like '%cannabis%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Cannabis Penny stocks' where ct.investcat ='penny' and ct.price <=5 and lower(ct.g_industry) like '%cannabis%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Cannabis stocks' where lower(ct.g_industry) like '%cannabis%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Cars stocks' where lower(ct.g_industry) like '%electr%vehic%' or lower(ct.g_industry) like '%auto%deal%' or lower(ct.g_industry) like '%auto%manufact%' or lower(ct.g_industry) like '%auto%marketpl%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Casino stocks' where lower(ct.g_industry) like '%casin%resort%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Cloud computing stocks' where lower(ct.g_industry) like '%enterpric%cloud%' or lower(ct.g_industry) like '%tech%conglomer%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Coffee stocks' where lower(ct.g_industry) like '%cof%e%tea%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Commodities stocks' where lower(ct.g_industry) like '%agricult%tech%' or lower(ct.g_industry) like '%bas%chemi%' or lower(ct.g_industry) like '%coal%' or lower(ct.g_industry) like '%energ%explor%' or lower(ct.g_industry) like '%industr%metal%' or lower(ct.g_industry) like '%liqui%natur%gas%' or lower(ct.g_industry) like '%mlp%' or lower(ct.g_industry) like '%oil%gas%produc%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Concrete stocks' where ticker_code in ('smid','uscr')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Construction stocks' where lower(ct.g_industry) like '%home%construct%maint%' or lower(ct.g_industry) like '%construct%infrastruct%' or lower(ct.g_industry) like '%build%mater%' or lower(ct.g_industry) like '%build%produc%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Consumer staples ETFs' where ct.ttype ='etf' and lower(ct.gics_sector) like '%consum%stapl%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Consumer staples stocks' where lower(ct.gics_sector) like '%consum%stapl%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Copper ETFs' where ct.ttype ='etf' and lower(ct.g_industry) like '%industr%metal%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Copper stocks' where lower(ct.g_industry) like '%industr%metal%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Cruise line ETFs' where ct.ttype ='etf' and (lower(ct.g_industry) like '%onlin%travel%compan%' or lower(ct.g_industry) like '%maritime%transport&')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Cruise line stocks' where lower(ct.g_industry) like '%onlin%travel%compan%' or lower(ct.g_industry) like '%maritime%transport&'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Data stocks' where lower(ct.g_industry) like '%analyt%ai%' or lower(ct.g_industry) like '%data%center%' and lower(ct.g_industry) not like '%reit%' or lower(ct.g_industry) like '%data%stor%manag%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Defense stocks' where lower(ct.g_industry) like '%aerospac%defen%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Development companies stocks' where ct.investcat ='defensive'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Discretionary consumer stocks' where lower(ct.gics_sector) like '%consum%discretionar%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Dividend Gold stocks' where lower(ct.g_industry) like '%gold%silver%' and lower(ct.investcat) ='dividend'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Drone stock companies' where lower(ct.g_industry) like '%drone%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'E-commerce and Retail stocks' where lower(ct.g_industry) like '%e-commerc%platform%' or lower(ct.g_industry) like '%e-commerc%operat%' or lower(ct.g_industry) like '%apparel%manufact%retail%' or lower(ct.g_industry) like '%discount%retail%' or lower(ct.g_industry) like '%home%furnit%retail%' or lower(ct.g_industry) like '%luxur%retail%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Education stocks' where lower(ct.g_industry) like '%educat%servic%' or lower(ct.g_industry) like '%educat%tech%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Electric vehicles stocks' where lower(ct.g_industry) like '%electr%vehicl%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Energy company ETFs' where ct.ttype ='etf' and (lower(ct.g_industry) like '%clean%energ%' or lower(ct.g_industry) like '%energ%explor%' or lower(ct.g_industry) like '%renew%energ%' or lower(ct.g_industry) like '%solar%energ%' or lower(ct.g_industry) like '%oil%gas%produc%' or lower(ct.g_industry) like '%liqui%natur%gas%' or lower(ct.g_industry) like '%electr%generat%' or lower(ct.g_industry) like '%power%gener%distrib%' or lower(ct.g_industry) like '%oil%gas%servic%' or lower(ct.g_industry) like '%electron%power%management%')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Energy company stocks' where lower(ct.g_industry) like '%clean%energ%' or lower(ct.g_industry) like '%energ%explor%' or lower(ct.g_industry) like '%renew%energ%' or lower(ct.g_industry) like '%solar%energ%' or lower(ct.g_industry) like '%oil%gas%produc%' or lower(ct.g_industry) like '%liqui%natur%gas%' or lower(ct.g_industry) like '%electr%generat%' or lower(ct.g_industry) like '%power%gener%distrib%' or lower(ct.g_industry) like '%oil%gas%servic%' or lower(ct.g_industry) like '%electron%power%management%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Energy Penny stocks' where ct.investcat ='penny' and ct.price <=5 and (lower(ct.g_industry) like '%clean%energ%' or lower(ct.g_industry) like '%energ%explor%' or lower(ct.g_industry) like '%renew%energ%' or lower(ct.g_industry) like '%solar%energ%' or lower(ct.g_industry) like '%oil%gas%produc%' or lower(ct.g_industry) like '%liqui%natur%gas%' or lower(ct.g_industry) like '%electr%generat%' or lower(ct.g_industry) like '%power%gener%distrib%' or lower(ct.g_industry) like '%oil%gas%servic%' or lower(ct.g_industry) like '%electron%power%management%')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Entertainment stocks' where lower(ct.g_industry) like '%tv%radio%' or lower(ct.g_industry) like '%streaming%' or lower(ct.g_industry) like '%gaming%esport%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Esport stocks' where ct.ticker_code in ('has', 'gmbl', 'slgg', 'inse', 'aese', 'ea', 'atvi', 'ttwo', 'dkng')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'EV batteries stocks' where lower(ct.g_industry) like '%batter%tech%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'FAANG (big tech) stocks' where lower(ct.g_industry) like '%tech%conglomerat%' or ct.ticker_code in ('fb', 'amzn', 'aapl', 'nflx', 'goog', 'googl')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Farming stocks' where lower(ct.g_industry) like '%agricultur%tech%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Fast food stocks' where lower(ct.g_industry) like '%casual%din%n%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Finance stocks' where lower(ct.gics_sector) like '%financ%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Fintech stocks' where lower(ct.g_industry) like '%financ%exchang%data%' or lower(ct.g_industry) like '%fintech%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Food Delivery stocks' where lower(ct.g_industry) like '%food%deliver%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Food stocks' where lower(ct.g_industry) like '%bread%condiment%' or lower(ct.g_industry) like '%casual%din%n%' or lower(ct.g_industry) like '%dairy%' or lower(ct.g_industry) like '%fruit%vegetabl%' or lower(ct.g_industry) like '%meat%poultry%' or lower(ct.g_industry) like '%snack%cand%' or lower(ct.g_industry) like '%soft%sport%drink%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Fuel stocks' where lower(ct.g_industry) like '%oil%gas%produc%' or lower(ct.g_industry) like '%liqui%natur%gas%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Furniture and decor stocks' where lower(ct.g_industry) like '%home%furnitur%retail%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Gambling stocks' where lower(ct.g_industry) like '%casin%resort%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Gaming companies stocks' where lower(ct.g_industry) like '%gam%esport%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Gold companies stocks' where lower(ct.g_industry) like '%gold%silver%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Green energy stocks' where lower(ct.g_industry) like '%clean%energ%' or lower(ct.g_industry) like '%energ%explor%' or lower(ct.g_industry) like '%renew%energ%' or lower(ct.g_industry) like '%solar%energ%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Gun stocks' where lower(ct.g_industry) like '%secur%protect%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Health insurance stocks' where lower(ct.g_industry) like '%health%insuranc%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Healthcare ETFs' where ct.ttype ='etf' and lower(ct.gics_sector) like '%health%car%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Healthcare Stocks' where lower(ct.gics_sector) like '%health%car%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Heavy Equipment Stocks' where lower(ct.g_industry) like '%construct%machine%' or lower(ct.g_industry) like '%heavy%machine%part%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Hotel stocks' where (lower(ct.g_industry) like '%hotel%' and lower(ct.g_industry) not like '%reit%') or lower(ct.g_industry) like '%casin%resort%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Information security stocks' where lower(ct.g_industry) like '%cyber%secur%' or lower(ct.g_industry) like '%ident%authenticat%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Infrastructure stocks' where lower(ct.g_industry) like '%construct%infrastruct%' or lower(ct.g_industry) like '%telecom%infrastruct%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Insurance stocks' where lower(ct.g_industry) like '%health%insuranc%' or lower(ct.g_industry) like '%insuranc%broker%' or lower(ct.g_industry) like '%insuranc%marketplac%tech%' or lower(ct.g_industry) like '%mortgag%insuranc%' or lower(ct.g_industry) like '%multiline%insuranc%' or lower(ct.g_industry) like '%property%casualt%insuranc%' or lower(ct.g_industry) like '%life%insuranc%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Internet of things stocks ' where lower(ct.g_industry) like 'iot%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Lithium stocks' where ct.ticker_code in ('ulbi', 'flux', 'tanh', 'pll', 'sedg')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Meat stocks' where lower(ct.g_industry) like '%meat%poultry%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Medical supplies company stocks' where lower(ct.g_industry) like '%health%care%service%equipment%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Mining companies stocks' where lower(ct.g_industry) like '%gold%silver%' or lower(ct.g_industry) like '%industr%metal%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'MLP to invest' where lower(ct.g_industry) like '%mlp%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Nanotechnology stocks' where lower(ct.g_industry) like '%nanotech%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Natural gas stocks' where lower(ct.g_industry) like '%oil%gas%produc%' or lower(ct.g_industry) like '%liqui%natur%gas%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Nuclear energy stocks' where ct.ticker_code in ('lbtr','aep','exc','xel','pesi','holi','gvp')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Oil ETF' where ct.ttype ='etf' and (lower(ct.g_industry) like '%oil%gas%produc%' or lower(ct.g_industry) like '%oil%gas%servic%')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Oil stocks' where lower(ct.g_industry) like '%oil%gas%produc%' or lower(ct.g_industry) like '%oil%gas%servic%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Penny stocks' where ct.investcat ='penny' and ct.price <=1.5 and ct.chrt <=0.2
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Pharmaceutical stocks' where lower(ct.g_industry) like '%pharm%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Precious metals stocks' where lower(ct.g_industry) like '%gold%silver%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Real estate stocks' where lower(ct.gics_sector) like '%real%estat%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Restaurant stocks' where lower(ct.g_industry) like '%gold%silver%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Robotics company stocks' where lower(ct.g_industry) like '%analytic%ai%' or lower(ct.g_industry) like '%comput%visi%sens%' or lower(ct.g_industry) like 'iot%' or lower(ct.g_industry) like '%precis%manufact%' or lower(ct.g_industry) like '%health%car%services%equipment%robot%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Self driving car stocks' where ct.ticker_code in ('lazr', 'f', 'aptv', 'tsla', 'googl', 'xpev', 'amba', 'qcom', 'nvda')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Semiconductors stocks' where lower(ct.g_industry) like '%semiconduct%equip%' or lower(ct.g_industry) like '%semiconduct%manufact%' or lower(ct.g_industry) like '%semiconduct%material%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Shipping stocks' where lower(ct.g_industry) like '%maritim%transport%' or lower(ct.g_industry) like '%truck%' or lower(ct.g_industry) like '%air%carg%transport%' or lower(ct.g_industry) like '%rail%freight%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Ships stocks' where lower(ct.g_industry) like '%maritim%transport%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Silver companies stocks' where lower(ct.g_industry) like '%gold%silver%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Silver ETFs' where ct.ttype ='etf' and lower(ct.g_industry) like '%gold%silver%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Space stocks' where lower(ct.g_industry) like '%aerospac%defen%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Steel stocks' where ct.ticker_code in ('zeus', 'schn', 'stld', 'usap', 'zkin')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Stocks in sports' where lower(ct.g_industry) like '%prosport%manag%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Technology stocks' where lower(ct.gics_sector) like '%technolog%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Telecom stocks' where lower(ct.g_industry) like '%5g%com%unicat%' or lower(ct.g_industry) like '%internet%cellular%provid%' or lower(ct.g_industry) like '%telecom%infrastruct%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Toilet paper companies stocks' where ct.ticker_code in ('wmt', 'pg', 'kmb', 'itp', 'clw', 'cost', 'kr')
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Transportation stocks' where lower(ct.g_industry) like '%maritim%transport%' or lower(ct.g_industry) like '%airline%' or lower(ct.g_industry) like '%air%carg%transport%' or lower(ct.g_industry) like '%rail%freight%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Travel stocks' where lower(ct.g_industry) like '%online%travel%compan%' or lower(ct.g_industry) like '%airline%' or lower(ct.g_industry) like '%casin%resort%' or lower(ct.g_industry) like '%hotel%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Trucking stocks' where lower(ct.g_industry) like '%truck%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Utilities ETFs' where ct.ttype ='etf' and lower(ct.gics_sector) like '%utilities%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Utilities stocks' where lower(ct.gics_sector) like '%utilities%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Waste management stocks' where lower(ct.g_industry) like '%wast%manag%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Water companies stocks' where lower(ct.g_industry) like '%water%manag%'
UNION
select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = 'Wine stocks' where lower(ct.g_industry) like '%wine%liquor%'
         )
SELECT t2.symbol, collection_id
from ticker_collections
         join {{ ref ('tickers') }} t2
on ticker_collections.symbol = t2.symbol
    join collections c2 on ticker_collections.collection_id = c2.id
