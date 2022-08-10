{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    post_hook=[
      pk('symbol'),
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}


with ticker_override as
         (
             select symbol,
                    trim(gic_sector)       as gic_sector,
                    trim(gic_group)        as gic_group,
                    trim(gic_industry)     as gic_industry,
                    trim(gic_sub_industry) as gic_sub_industry,
                    trim(description) as description
             from {{ source('gainy', 'ticker_override')}}
             where _sdc_extracted_at > (
                                           select max(_sdc_extracted_at)
                                           from {{ source('gainy', 'ticker_override')}}
                                       ) - interval '1 minute'
         ),
     eod_tickers as
         (
             select upper(eod_fundamentals.code)::varchar           as symbol,
                    lower(general ->> 'Type')::character varying    as type,
                    (general ->> 'Name')::character varying         as name,
                    coalesce(ticker_override.description,
                             general ->> 'Description',
                             regexp_replace(
                                 replace(
                                     crypto_coins.description ->> 'en',
                                     '\r\n',
                                     E'\n'
                                 ),
                                 '<[^<>]*>',
                                 '',
                                 'g'
                             )
                        )::varchar                                  as description,
                    (general ->> 'Phone')::character varying        as phone,
                    (general ->> 'LogoURL')::character varying      as logo_url,
                    (general ->> 'WebURL')::character varying       as web_url,
                    (general ->> 'IPODate')::date                   as ipo_date,
                    (general ->> 'Sector')::character varying       as sector,
                    (general ->> 'Industry')::character varying     as industry,
                    coalesce(ticker_override.gic_sector,
                             general ->> 'GicSector')::varchar      as gic_sector,
                    coalesce(ticker_override.gic_group,
                             general ->> 'GicGroup')::varchar       as gic_group,
                    coalesce(ticker_override.gic_industry,
                             general ->> 'GicIndustry')::varchar    as gic_industry,
                    coalesce(ticker_override.gic_sub_industry,
                             general ->> 'GicSubIndustry')::varchar as gic_sub_industry,
                    (general ->> 'Exchange')::character varying     as exchange,
                    case
                        when general ->> 'Exchange' != 'INDX'
                            then (string_to_array(general ->> 'Exchange', ' '))[1]::varchar
                        end                                         as exchange_canonical,
                    -- TODO while this is good enough for Europe, China and LatAm - other countries should be rechecked.
                    --  For instance "South Korea" is not the official name, as well as "United States"
                    trim(coalesce(
                                    general -> 'AddressData' ->>
                                    'Country', -- it's good but there are 65 tickets without it set
                                    case
                                        when TRIM(both
                                                  reverse(split_part(reverse(general ->> 'Address'), ',', 1))) ~
                                             '[0-9]'
                                            then TRIM(both
                                                      reverse(split_part(reverse(general ->> 'Address'), ',', 2)))
                                        else TRIM(both
                                                  reverse(split_part(reverse(general ->> 'Address'), ',', 1)))
                                        end, -- fallback if previous one is null
                                    general ->> 'CountryName' -- it's USA for all companies in EOD
                        ))                                          as country_name,
                    (general ->> 'UpdatedAt')::timestamp            as updated_at
             from {{ source('eod', 'eod_fundamentals')}}
                      left join {{ ref('crypto_coins') }} on crypto_coins.symbol = upper(eod_fundamentals.code)
                      left join ticker_override
                                on ticker_override.symbol = upper(eod_fundamentals.code)
             where ((general ->> 'IsDelisted') is null or (general ->> 'IsDelisted')::bool = false)
               and _sdc_extracted_at > (select max(_sdc_extracted_at)::date from {{ source('eod', 'eod_fundamentals')}}) - interval '1 day'
               and eod_fundamentals.code not in
                   ('NTEST', 'NTEST-M', 'PTEST', 'MTEST', 'PTEST-W', 'PTEST-X', 'ATEST-A', 'ATEST-G',
                    'ATEST-C', 'ZZZ', 'ZJZZT', 'ATEST-H', 'ZVV', 'NTEST-G', 'NTEST-J', 'NTEST-K', 'ZXYZ-A',
                    'NTEST-Y', 'NTEST-H', 'NTEST-I', 'NTEST-O', 'NTEST-Z', 'CTEST', 'CBX', 'IGZ', 'NTEST-L',
                    'NTEST-Q', 'CBO', 'ZWZZT', 'ZVZZT')
         ),
     coingecko_tickers as
         (
             select symbol,
                    'crypto'                            as type,
                    name,
                    regexp_replace(
                        replace(
                            crypto_coins.description ->> 'en',
                            '\r\n',
                            E'\n'
                        ),
                        '<[^<>]*>',
                        '',
                        'g'
                    )                                   as description,
                    null                                as phone,
                    (image ->> 'large')                 as logo_url,
                    (links -> 'homepage' ->> 0)         as web_url,
                    (ico_data ->> 'ico_end_date')::date as ipo_date,
                    null                                as sector,
                    null                                as industry,
                    null                                as gic_sector,
                    null                                as gic_group,
                    null                                as gic_industry,
                    null                                as gic_sub_industry,
                    null                                as exchange,
                    null                                as exchange_canonical,
                    null                                as country_name,
                    updated_at
             from {{ ref('crypto_coins') }}
         )
select distinct on (symbol) *
from (
         select symbol,
                type,
                name,
                description,
                phone,
                logo_url,
                web_url,
                ipo_date,
                sector,
                industry,
                gic_sector,
                gic_group,
                gic_industry,
                gic_sub_industry,
                country_name,
                exchange,
                exchange_canonical,
                now() as updated_at
         from eod_tickers

         union all

         select symbol,
                type,
                name,
                description,
                phone,
                logo_url,
                web_url,
                ipo_date,
                sector,
                industry,
                gic_sector,
                gic_group,
                gic_industry,
                gic_sub_industry,
                country_name,
                exchange,
                exchange_canonical,
                now() as updated_at
         from coingecko_tickers
     ) t
