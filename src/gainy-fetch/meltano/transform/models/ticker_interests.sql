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
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Cars Manufacturers' where  lower(ct.g_industry) like '%electr%vehic%' or lower(ct.g_industry) like '%auto%deal%' or lower(ct.g_industry) like '%auto%manufact%' or lower(ct.g_industry) like '%auto%marketpl%' or lower(ct.g_industry) like '%auto%part%service%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Cable & Satellite' where  lower(ct.g_industry) like '%satel%comm%' or lower(ct.g_industry) like '%tv%radio%' or lower(ct.g_industry) like '%5g%comm%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Gaming' where  lower(ct.g_industry) like '%gam%esport%' or lower(ct.g_industry) like '%casin%resort%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Cannabis' where  lower(ct.g_industry) like '%cannabis%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Consumer Electronics' where  lower(ct.g_industry) like '%electron%displ%' or lower(ct.g_industry) like '%electron%manufact%' or lower(ct.g_industry) like '%drones%' or lower(ct.g_industry) like 'iot%' or lower(ct.g_industry) like '%tech%conglomerat%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Hotels & Cruise Lines' where  lower(ct.g_industry) like '%hotel%' and lower(ct.g_industry) not like '%reit%' or lower(ct.g_industry) like '%onlin%travel%compan%' or lower(ct.g_industry) like '%maritime%transport%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Peloton' where  lower(ct.g_industry) like '%outdoor%life%style%' or lower(ct.g_industry) like '%sport%drink%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Crypto' where  lower(ct.g_industry) like '%cryptocurrenc%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Green Energy' where  lower(ct.g_industry) like '%clean%energ%' or lower(ct.g_industry) like '%energ%explor%' or lower(ct.g_industry) like '%renew%energ%' or lower(ct.g_industry) like '%solar%energ%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Health Insurance' where  lower(ct.g_industry) like '%health%insur%' or lower(ct.g_industry) like '%life%insur%' or lower(ct.g_industry) like '%multilin%insur%' or lower(ct.g_industry) like '%insur%broker%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Publishing' where  lower(ct.g_industry) like '%digit%content%publish%' or lower(ct.g_industry) like '%publishers%' or lower(ct.g_industry) like '%digit%market%advert%' or lower(ct.g_industry) like '%digit%media%web%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Technology Hardware' where  lower(ct.g_industry) like '%5g%comm%' or lower(ct.g_industry) like '%comput%vision%sens%' or lower(ct.g_industry) like '%data%stor%manag%' or lower(ct.g_industry) like '%electron%compon%' or lower(ct.g_industry) like '%electron%displ%' or lower(ct.g_industry) like '%electron%manufact%' or lower(ct.g_industry) like '%memor%stor%' or lower(ct.g_industry) like '%network%equip%' or lower(ct.g_industry) like '%power%generat%distrib%' or lower(ct.g_industry) like '%video%tech%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Trucking' where  lower(ct.g_industry) like '%truck%' or lower(ct.g_industry) like '%auto%part%serv%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Salesforce' where  lower(ct.g_industry) like '%crm%customer%succ%' or lower(ct.g_industry) like '%distrib%work%tool%' or lower(ct.g_industry) like '%enterpris%tech%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'AI' where  lower(ct.g_industry) like '%analyt%ai%' or lower(ct.g_industry) like '%comput%vis%sens%' or lower(ct.g_industry) like 'iot%' or lower(ct.g_industry) like '%precis%manufact%' or lower(ct.g_industry) like '%health%car%serv%equip%robot%' or lower(ct.g_industry) like '%tech%conglomerat%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Airlines' where  lower(ct.g_industry) like '%airline%' or lower(ct.g_industry) like '%air%carg%transport%' or lower(ct.g_industry) like '%aircraft%leas%' or lower(ct.g_industry) like '%onlin%travel%comp%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Entertainment' where  lower(ct.g_industry) like '%tv%radio%' or lower(ct.g_industry) like '%streaming%' or lower(ct.g_industry) like '%gaming%esport%' or lower(ct.g_industry) like '%digit%market%advert%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Biotechnology' where  lower(ct.g_industry) like '%biotech%' or lower(ct.g_industry) like '%pharm%cancer%diseas%' or lower(ct.g_industry) like '%health%car%serv%equip%lab%' or lower(ct.g_industry) like '%pharm%covid%' or lower(ct.g_industry) like '%pharm%dermat%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Banks' where  lower(ct.g_industry) like 'bank%' or lower(ct.g_industry) like '%invest%bank%adv%' or lower(ct.g_industry) like '%invest%manag%' or lower(ct.g_industry) like '%trad%privat%invest%fund%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Lululemon' where  lower(ct.g_industry) like '%luxur%retail%' or lower(ct.g_industry) like '%apparel%manufact%retail%' or lower(ct.g_industry) like '%outdoor%life%styl%prod%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Ecommerce' where  lower(ct.g_industry) like '%e-commerc%operat%' or lower(ct.g_industry) like '%e-commerc%platform%' or lower(ct.g_industry) like '%tech%conglomerat%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Coursera' where  lower(ct.g_industry) like '%educat%serv%' or lower(ct.g_industry) like '%educat%tech%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'PayPal' where  lower(ct.g_industry) like '%payment%system%' or lower(ct.g_industry) like '%fintech%operat%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Casino' where  lower(ct.g_industry) like '%casin%resort%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Gold & Silver' where  lower(ct.g_industry) like '%gold%silver%' or lower(ct.g_industry) like '%prec%metal%trad%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Homebuilding' where  lower(ct.g_industry) like '%home%construct%maint%' or lower(ct.g_industry) like '%build%mater%' or lower(ct.g_industry) like '%build%prod%' or lower(ct.g_industry) like '%home%furnit%retail%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Oil & Gas' where  lower(ct.g_industry) like '%oil%gas%produc%' or lower(ct.g_industry) like '%liqui%natur%gas%' or lower(ct.g_industry) like '%oil%gas%servic%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Internet Infrastructure' where  lower(ct.g_industry) like '%cybersecur%' or lower(ct.g_industry) like '%data%cent%' or lower(ct.g_industry) like '%domain%reg%' or lower(ct.g_industry) like '%enterpris%cloud%' or lower(ct.g_industry) like '%network%equip%' or lower(ct.g_industry) like '%tech%conglomerat%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Real Estate' where  lower(ct.g_industry) like '%real%estat%tech%' or lower(ct.g_industry) like '%real%estat%prop%manag%' or lower(ct.g_industry) like '%real%estat%invest%develop%' or lower(ct.g_industry) like '%commerc%real%estat%broker%serv%' or lower(ct.g_industry) like '% reit'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Restaurants' where  lower(ct.g_industry) like '%casual%din%' or lower(ct.g_industry) like '%food%deliver%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Semiconductors' where  lower(ct.g_industry) like '%semiconduct%equip%' or lower(ct.g_industry) like '%semiconduct%manufact%' or lower(ct.g_industry) like '%semiconduct%mater%' or lower(ct.g_industry) like '%memory%storage%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'SPACs' where  lower(ct.g_industry) like '%shell%compan%spac%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Social media' where  lower(ct.g_industry) like '%social%media%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Utilities' where  lower(ct.g_industry) like '%electricity%generat%' or lower(ct.g_industry) like '%waste%manag%' or lower(ct.g_industry) like '%water%manag%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Tesla' where  lower(ct.g_industry) like '%electric%vehicle%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Gig Economy' where  lower(ct.g_industry) like '%payment%system%' or lower(ct.g_industry) like '%shar%econom%' or lower(ct.g_industry) like '%e-commerc%platform%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Sharing economy' where  lower(ct.g_industry) like '%shar%econom%'
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Beverages' where  lower(ct.g_industry) like '%cof%e%tea%' or lower(ct.g_industry) like '%wine%liquor%' or lower(ct.g_industry) like '%soft%sport%drink%' 
UNION
select interests.id as interest_id, ct.ticker_code as symbol from ct join interests on interests.name = 'Netflix' where  lower(ct.g_industry) like 'streaming%'
         )
SELECT t2.symbol, interest_id
from tmp_ticker_interests
    join {{ ref ('tickers') }} t2 on tmp_ticker_interests.symbol = t2.symbol
    join {{ ref ('interests') }} c2 on tmp_ticker_interests.interest_id = c2.id
