{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    post_hook=[
      pk('symbol'),
    ]
  )
}}


with ticker_gic_override as
         (
             select symbol,
                    trim(gic_sector)       as gic_sector,
                    trim(gic_group)        as gic_group,
                    trim(gic_industry)     as gic_industry,
                    trim(gic_sub_industry) as gic_sub_industry
             from {{ source('gainy', 'ticker_gic_override') }}
             where _sdc_extracted_at > (select max(_sdc_extracted_at) from {{ source('gainy', 'ticker_gic_override') }}) - interval '1 minute'
         )
select upper(eod_fundamentals.code)::varchar           as symbol,
       lower(general ->> 'Type')::character varying    as type,
       (general ->> 'Name')::character varying         as name,
       coalesce(general -> 'Description',
                crypto_coins.description -> 'en'
           )::varchar                                  as description,
       (general ->> 'Phone')::character varying        as phone,
       (general ->> 'LogoURL')::character varying      as logo_url,
       (general ->> 'WebURL')::character varying       as web_url,
       (general ->> 'IPODate')::date                   as ipo_date,
       (general ->> 'Sector')::character varying       as sector,
       (general ->> 'Industry')::character varying     as industry,
       coalesce(ticker_gic_override.gic_sector,
                general ->> 'GicSector')::varchar      as gic_sector,
       coalesce(ticker_gic_override.gic_group,
                general ->> 'GicGroup')::varchar       as gic_group,
       coalesce(ticker_gic_override.gic_industry,
                general ->> 'GicIndustry')::varchar    as gic_industry,
       coalesce(ticker_gic_override.gic_sub_industry,
                general ->> 'GicSubIndustry')::varchar as gic_sub_industry,
       (general ->> 'Exchange')::character varying     as exchange,
       case
           when general ->> 'Exchange' != 'INDX'
               then (string_to_array(general ->> 'Exchange', ' '))[1]::varchar
           end                                         as exchange_canonical,
       -- TODO while this is good enough for Europe, China and LatAm - other countries should be rechecked.
       --  For instance "South Korea" is not the official name, as well as "United States"
       trim(coalesce(
                       general -> 'AddressData' ->> 'Country', -- it's good but there are 65 tickets without it set
                       case
                           when TRIM(both reverse(split_part(reverse(general ->> 'Address'), ',', 1))) ~ '[0-9]'
                               then TRIM(both reverse(split_part(reverse(general ->> 'Address'), ',', 2)))
                           else TRIM(both reverse(split_part(reverse(general ->> 'Address'), ',', 1)))
                           end, -- fallback if previous one is null
                       general ->> 'CountryName' -- it's USA for all companies in EOD
           ))                                          as country_name,
       (general ->> 'UpdatedAt')::timestamp            as updated_at
from {{ source('eod', 'eod_fundamentals') }}
left join {{ ref('crypto_coins') }} on crypto_coins.symbol = upper(eod_fundamentals.code)
left join ticker_gic_override on ticker_gic_override.symbol = upper(eod_fundamentals.code)
where ((general ->> 'IsDelisted') is null or (general ->> 'IsDelisted')::bool = false)
  and eod_fundamentals.code not in ('ZWZZT', 'ZVZZT')
