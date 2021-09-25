{{
  config(
    materialized = "table",
    indexes = [
      { 'columns': ['interest_id', 'symbol'], 'unique': true },
    ],
    post_hook=[
      fk(this, 'interest_id', 'interests', 'id'),
      fk(this, 'symbol', 'tickers', 'symbol')
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
     tmp_ticker_interests as
         (
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Cars Manufacturers' where ct.g_industry ilike '%electr%vehic%' or ct.g_industry ilike '%auto%deal%' or ct.g_industry ilike '%auto%manufact%' or ct.g_industry ilike '%auto%marketpl%' or ct.g_industry ilike '%auto%part%service%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Cable & Satellite' where ct.g_industry ilike '%satel%comm%' or ct.g_industry ilike '%tv%radio%' or ct.g_industry ilike '%5g%comm%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Gaming' where ct.g_industry ilike '%gam%esport%' or ct.g_industry ilike '%casin%resort%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Cannabis' where ct.g_industry ilike '%cannabis%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Consumer Electronics' where ct.g_industry ilike '%electron%displ%' or ct.g_industry ilike '%electron%manufact%' or ct.g_industry ilike '%drones%' or ct.g_industry ilike 'iot%' or ct.g_industry ilike '%tech%conglomerat%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Hotels & Cruise Lines' where ct.g_industry ilike '%hotel%' and ct.g_industry not ilike '%reit%' or ct.g_industry ilike '%onlin%travel%compan%' or ct.g_industry ilike '%maritime%transport%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Peloton' where ct.g_industry ilike '%outdoor%life%style%' or ct.g_industry ilike '%sport%drink%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Crypto' where ct.g_industry ilike '%cryptocurrenc%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Green Energy' where ct.g_industry ilike '%clean%energ%' or ct.g_industry ilike '%energ%explor%' or ct.g_industry ilike '%renew%energ%' or ct.g_industry ilike '%solar%energ%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Health Insurance' where ct.g_industry ilike '%health%insur%' or ct.g_industry ilike '%life%insur%' or ct.g_industry ilike '%multilin%insur%' or ct.g_industry ilike '%insur%broker%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Publishing' where ct.g_industry ilike '%digit%content%publish%' or ct.g_industry ilike '%publishers%' or ct.g_industry ilike '%digit%market%advert%' or ct.g_industry ilike '%digit%media%web%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Technology Hardware' where ct.g_industry ilike '%5g%comm%' or ct.g_industry ilike '%comput%vision%sens%' or ct.g_industry ilike '%data%stor%manag%' or ct.g_industry ilike '%electron%compon%' or ct.g_industry ilike '%electron%displ%' or ct.g_industry ilike '%electron%manufact%' or ct.g_industry ilike '%memor%stor%' or ct.g_industry ilike '%network%equip%' or ct.g_industry ilike '%power%generat%distrib%' or ct.g_industry ilike '%video%tech%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Trucking' where ct.g_industry ilike '%truck%' or ct.g_industry ilike '%auto%part%serv%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Salesforce' where ct.g_industry ilike '%crm%customer%succ%' or ct.g_industry ilike '%distrib%work%tool%' or ct.g_industry ilike '%enterpris%tech%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'AI' where ct.g_industry ilike '%analyt%ai%' or ct.g_industry ilike '%comput%vis%sens%' or ct.g_industry ilike 'iot%' or ct.g_industry ilike '%precis%manufact%' or ct.g_industry ilike '%health%car%serv%equip%robot%' or ct.g_industry ilike '%tech%conglomerat%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Airlines' where ct.g_industry ilike '%airline%' or ct.g_industry ilike '%air%carg%transport%' or ct.g_industry ilike '%aircraft%leas%' or ct.g_industry ilike '%onlin%travel%comp%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Entertainment' where ct.g_industry ilike '%tv%radio%' or ct.g_industry ilike '%streaming%' or ct.g_industry ilike '%gaming%esport%' or ct.g_industry ilike '%digit%market%advert%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Biotechnology' where ct.g_industry ilike '%biotech%' or ct.g_industry ilike '%pharm%cancer%diseas%' or ct.g_industry ilike '%health%car%serv%equip%lab%' or ct.g_industry ilike '%pharm%covid%' or ct.g_industry ilike '%pharm%dermat%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Banks' where ct.g_industry ilike 'bank%' or ct.g_industry ilike '%invest%bank%adv%' or ct.g_industry ilike '%invest%manag%' or ct.g_industry ilike '%trad%privat%invest%fund%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Lululemon' where ct.g_industry ilike '%luxur%retail%' or ct.g_industry ilike '%apparel%manufact%retail%' or ct.g_industry ilike '%outdoor%life%styl%prod%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Ecommerce' where ct.g_industry ilike '%e-commerc%operat%' or ct.g_industry ilike '%e-commerc%platform%' or ct.g_industry ilike '%tech%conglomerat%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Coursera' where ct.g_industry ilike '%educat%serv%' or ct.g_industry ilike '%educat%tech%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'PayPal' where ct.g_industry ilike '%payment%system%' or ct.g_industry ilike '%fintech%operat%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Casino' where ct.g_industry ilike '%casin%resort%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Gold & Silver' where ct.g_industry ilike '%gold%silver%' or ct.g_industry ilike '%prec%metal%trad%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Homebuilding' where ct.g_industry ilike '%home%construct%maint%' or ct.g_industry ilike '%build%mater%' or ct.g_industry ilike '%build%prod%' or ct.g_industry ilike '%home%furnit%retail%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Oil & Gas' where ct.g_industry ilike '%oil%gas%produc%' or ct.g_industry ilike '%liqui%natur%gas%' or ct.g_industry ilike '%oil%gas%servic%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Internet Infrastructure' where ct.g_industry ilike '%cybersecur%' or ct.g_industry ilike '%data%cent%' or ct.g_industry ilike '%domain%reg%' or ct.g_industry ilike '%enterpris%cloud%' or ct.g_industry ilike '%network%equip%' or ct.g_industry ilike '%tech%conglomerat%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Real Estate' where ct.g_industry ilike '%real%estat%tech%' or ct.g_industry ilike '%real%estat%prop%manag%' or ct.g_industry ilike '%real%estat%invest%develop%' or ct.g_industry ilike '%commerc%real%estat%broker%serv%' or ct.g_industry ilike '% reit'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Restaurants' where ct.g_industry ilike '%casual%din%' or ct.g_industry ilike '%food%deliver%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Semiconductors' where ct.g_industry ilike '%semiconduct%equip%' or ct.g_industry ilike '%semiconduct%manufact%' or ct.g_industry ilike '%semiconduct%mater%' or ct.g_industry ilike '%memory%storage%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'SPACs' where ct.g_industry ilike '%shell%compan%spac%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Social media' where ct.g_industry ilike '%social%media%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Utilities' where ct.g_industry ilike '%electricity%generat%' or ct.g_industry ilike '%waste%manag%' or ct.g_industry ilike '%water%manag%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Tesla' where ct.g_industry ilike '%electric%vehicle%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Gig Economy' where ct.g_industry ilike '%payment%system%' or ct.g_industry ilike '%shar%econom%' or ct.g_industry ilike '%e-commerc%platform%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Sharing economy' where ct.g_industry ilike '%shar%econom%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Beverages' where ct.g_industry ilike '%cof%e%tea%' or ct.g_industry ilike '%wine%liquor%' or ct.g_industry ilike '%soft%sport%drink%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Netflix' where ct.g_industry ilike 'streaming%'
         )
SELECT t2.symbol, interest_id
from tmp_ticker_interests
    join {{ ref ('tickers') }} t2 on tmp_ticker_interests.symbol = t2.symbol
    join {{ ref ('interests') }} c2 on tmp_ticker_interests.interest_id = c2.id
