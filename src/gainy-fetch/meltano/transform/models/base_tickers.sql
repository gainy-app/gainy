{{
  config(
    materialized = "view"
  )
}}

select (general ->> 'Code')::character varying           as symbol,
       (general ->> 'Type')::character varying           as type,
       (general ->> 'Name')::character varying           as name,
       (general -> 'Description')::character varying     as description,
       (general ->> 'Phone')::character varying          as phone,
       (general ->> 'LogoURL')::character varying        as logo_url,
       (general ->> 'WebURL')::character varying         as web_url,
       (general ->> 'IPODate')::date                     as ipo_date,
       (general ->> 'Sector')::character varying         as sector,
       (general ->> 'Industry')::character varying       as industry,
       (general ->> 'GicSector')::character varying      as gic_sector,
       (general ->> 'GicGroup')::character varying       as gic_group,
       (general ->> 'GicIndustry')::character varying    as gic_industry,
       (general ->> 'GicSubIndustry')::character varying as gic_sub_industry,
       (general ->> 'Exchange')::character varying       as exchange,
       -- TODO while this is good enough for Europe, China and LatAm - other countries should be rechecked.
       --  For instance "South Korea" is not the official name, as well as "United States"
       coalesce(
                       general -> 'AddressData' ->> 'Country', -- it's good but there are 65 tickets without it set
                       case
                           when TRIM(both reverse(split_part(reverse(general ->> 'Address'), ',', 1))) ~ '[0-9]'
                               then TRIM(both reverse(split_part(reverse(general ->> 'Address'), ',', 2)))
                           else TRIM(both reverse(split_part(reverse(general ->> 'Address'), ',', 1)))
                           end, -- fallback if previous one is null
                       general ->> 'CountryName' -- it's USA for all companies in EOD
           )                                             as country_name,
       (general ->> 'UpdatedAt')::timestamp              as updated_at
from {{ source('eod', 'eod_fundamentals') }}
where ((general ->> 'IsDelisted') is null or (general ->> 'IsDelisted')::bool = false)
