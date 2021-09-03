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

/* 'ALTER TABLE IF EXISTS app.collection_symbols add constraint collection_symbols_tickers_symbol_fk foreign key (symbol) references tickers (symbol)' */

select (general->>'Code')::text           as symbol,
       (general->>'Type')::text           as type,
       (general->>'Name')::text           as name,
       (general->'Description')::text     as description,
       (general->>'Phone')::text          as phone,
       (general->>'LogoURL')::text        as logo_url,
       (general->>'WebURL')::text         as web_url,
       (general->>'IPODate')::date        as ipo_date,
       (general->>'Sector')::text         as sector,
       (general->>'Industry')::text       as industry,
       (general->>'GicSector')::text      as gic_sector,
       (general->>'GicGroup')::text       as gic_group,
       (general->>'GicIndustry')::text    as gic_industry,
       (general->>'GicSubIndustry')::text as gic_sub_industry,
       (general->>'UpdatedAt')::timestamp as updated_at
from fundamentals
where
    (general->>'IsDelisted')::boolean = false and
    (general->>'Sector')::text is not null
