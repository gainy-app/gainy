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

select code::text as symbol,
       name::text as name,
       description::text as description,
       phone::text as phone,
       logourl::text as logo_url,
       ipodate::date as ipo_date,
       gicsector::text as sector,
       gicindustry::text as industry,
       gicsubindustry::text as sub_industry,
       updatedat::timestamp as updated_at
from {{ ref('general') }}
where
    isdelisted::boolean = false and
    gicsector::text is not null
