{{
  config(
    materialized = "table",
    sort = "updated_at",
    dist = "symbol",
    post_hook=[
      index(this, 'symbol', true),
    ]
  )
}}

select (general->>'Code')::character varying           as symbol,
       (general->>'Type')::character varying           as type,
       (general->>'Name')::character varying           as name,
       (general->'Description')::character varying     as description,
       (general->>'Phone')::character varying          as phone,
       (general->>'LogoURL')::character varying        as logo_url,
       (general->>'WebURL')::character varying         as web_url,
       (general->>'IPODate')::date                     as ipo_date,
       (general->>'Sector')::character varying         as sector,
       (general->>'Industry')::character varying       as industry,
       (general->>'GicSector')::character varying      as gic_sector,
       (general->>'GicGroup')::character varying       as gic_group,
       (general->>'GicIndustry')::character varying    as gic_industry,
       (general->>'GicSubIndustry')::character varying as gic_sub_industry,
       (general->>'CountryName')::character varying    as country_name,
       (general->>'UpdatedAt')::timestamp              as updated_at
from fundamentals
where
    (general->>'IsDelisted')::boolean = false and
    (general->>'Sector')::character varying is not null
