{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    post_hook=[
      index(this, 'symbol', true),
    ]
  )
}}

select
    upper(eod_fundamentals.code)::varchar as symbol,
    lower(eod_fundamentals.general ->> 'Type')::character varying as type,
    (eod_fundamentals.general ->> 'Name')::character varying as name,
    coalesce(eod_fundamentals.general -> 'Description',
        crypto_coins.description -> 'en'
    )::varchar as description,
    (eod_fundamentals.general ->> 'Phone')::character varying as phone,
    (eod_fundamentals.general ->> 'LogoURL')::character varying as logo_url,
    (eod_fundamentals.general ->> 'WebURL')::character varying as web_url,
    (eod_fundamentals.general ->> 'IPODate')::date as ipo_date,
    (eod_fundamentals.general ->> 'Sector')::character varying as sector,
    (eod_fundamentals.general ->> 'Industry')::character varying as industry,
    (eod_fundamentals.general ->> 'GicSector')::character varying as gic_sector,
    (eod_fundamentals.general ->> 'GicGroup')::character varying as gic_group,
    (eod_fundamentals.general ->> 'GicIndustry')::character varying as gic_industry,
    (eod_fundamentals.general ->> 'GicSubIndustry')::character varying as gic_sub_industry,
    (eod_fundamentals.general ->> 'Exchange')::character varying as exchange,
    case
        when eod_fundamentals.general ->> 'Exchange' != 'INDX'
            then (string_to_array(eod_fundamentals.general ->> 'Exchange', ' '))[1]::varchar
    end as exchange_canonical,
    -- TODO while this is good enough for Europe, China and LatAm - other countries should be rechecked.
    --  For instance "South Korea" is not the official name, as well as "United States"
    trim(coalesce(
        -- it's good but there are 65 tickets without it set
        eod_fundamentals.general -> 'AddressData' ->> 'Country',
        case
            when trim(reverse(split_part(reverse(eod_fundamentals.general ->> 'Address'), ',', 1))) ~ '[0-9]'
                then reverse(split_part(reverse(eod_fundamentals.general ->> 'Address'), ',', 2))
            else
                reverse(split_part(reverse(eod_fundamentals.general ->> 'Address'), ',', 1))
        end, -- fallback if previous one is null
        -- it's USA for all companies in EOD
        eod_fundamentals.general ->> 'CountryName'
    )) as country_name,
    (eod_fundamentals.general ->> 'UpdatedAt')::timestamp as updated_at
from {{ source('eod', 'eod_fundamentals') }}
left join {{ ref('crypto_coins') }} on crypto_coins.symbol = upper(eod_fundamentals.code)
where ((eod_fundamentals.general ->> 'IsDelisted') is null or (eod_fundamentals.general ->> 'IsDelisted')::bool = false)
