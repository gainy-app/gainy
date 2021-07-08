{{
  config(
    materialized = "table",
    sort = "updated_at",
    dist = "symbol",
    post_hook=[
      index(this, 'symbol', true)
    ]
  )
}}

select (general->>'Code')::text as symbol,
       (general->>'Name')::text as name,
       (general->'Description')::text as description,
       (general->>'Phone')::text as phone,
       (general->>'LogoUrl')::text as logo_url,
       (general->>'IPODate')::date as ipo_date,
       (general->>'GicSector')::text as sector,
       (general->>'GicIndustry')::text as industry,
       (general->>'GicSubIndustry')::text as sub_industry,
       (general->>'UpdatedAt')::timestamp as updated_at
from fundamentals
where
    (general->>'IsDelisted')::boolean = false and
    (general->>'GicSector')::text is not null
