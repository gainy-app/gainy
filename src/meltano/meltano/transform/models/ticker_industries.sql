{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists "industry_id__symbol" ON {{ this }} (industry_id, symbol)',
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}

with common_stocks as
         (
             select *
             from {{ ref('tickers') }}
             where type = 'common stock'
         ),

     ticker_auto_industries as (
       select ati.*
       from (
         -- union stack for the future: in case we would like more softeness on interests/industries we can use more top industries from the the model here (by union that blocks)
         -- 1st auto industry
         (select ati.symbol            as symbol,
                 ati.industry_id_1     as industry_id,
                 ati.industry_1_cossim as sim,
                 1::int                as industry_preorder
          from {{ source('gainy', 'auto_ticker_industries') }} ati)
         -- 2nd auto industry
         union
         (select ati.symbol            as symbol,
                 ati.industry_id_2     as industry_id,
                 ati.industry_2_cossim as sim,
                 2::int                as industry_preorder
          from {{ source('gainy', 'auto_ticker_industries') }} ati)
       ) as ati
       
       join common_stocks using (symbol) -- autogenerator generates for many types of tickers, so filter it
     ),

-- manual industries, don't touch it
     ticker_manual_industries as (
         select rgti.code  as symbol,
                gi.id::int as industry_id,
                (max(ai.sim) + (1.-max(ai.sim))/2.)::float 	as sim, -- we completely sure about manual marked industries, so put it in between of limit=1 and max one industry autogenerated
                0::int     as industry_preorder -- sim theoretically could be equal, so lets presave logical order to sort final industry order by this field
         from {{ source('gainy', 'gainy_ticker_industries') }} rgti -- 1 row
                  join {{ ref('gainy_industries') }} gi on gi."name" = rgti."industry name" -- 1 row
                  join ticker_auto_industries ai on ai.symbol = rgti.code -- to add (1-max(ai.sim))/2 delta (and inner join coz some manually marked tickers could be delisted and now absent in ai)
         group by rgti.code, gi.id::int -- to add (1-max(ai.sim))/2 delta
     ),

-- step 1. overwrite similarity and preorder if manual industry is in auto industries of the ticker (making that manual one as top 1st industry)
     ticker_industries_mi_over_ai as (
         select ai.symbol,
                ai.industry_id,
                coalesce(mi.sim, ai.sim)                             as sim,
                coalesce(mi.industry_preorder, ai.industry_preorder) as industry_preorder
         from ticker_auto_industries ai
                  left join ticker_manual_industries mi on mi.symbol = ai.symbol
             and mi.industry_id = ai.industry_id
     ),

-- step 2. after step 1, for all tickers where manual industry is defined but it still not in adjusted industries list of the ticker:
--         we need to shift down sequence of its industries and put manual on top of the sequence
     ticker_symbols_for_shift as ( -- ticker symbols for shifting
         select distinct mi.symbol
         from ticker_manual_industries mi
                  join ticker_industries_mi_over_ai using (symbol)
         where not exists(select 1
                          from ticker_industries_mi_over_ai miovai
                          where mi.symbol = miovai.symbol
                            and mi.industry_id = miovai.industry_id)
     ),

	-- union 3 parts
     ticker_industries_union as (
        (select miovai.* from ticker_industries_mi_over_ai miovai 	-- 1. plain auto and manual overwrited auto (excluding toshift part)
         left join ticker_symbols_for_shift sfs on sfs.symbol = miovai.symbol 
         where sfs.symbol is null )
        union 
         (select miovai.* from ticker_industries_mi_over_ai miovai  -- 2. toshift part 1: drop the last industry (industry_preorder < max(industry_preorder) (that "max" gives the actual length of the industries sequence that model saves to ticker_auto_industries, so we are able to set the model to output and save more lenghty industries sequences, to be able to get more softeness on interests/industries match component))
         join ticker_symbols_for_shift sfs on sfs.symbol = miovai.symbol
         where miovai.industry_preorder < (select max(industry_preorder) from ticker_auto_industries) ) -- if model somehow gives not length-equal sequences then we save here, coz for low-length sequences the manual industry will be just added without dropping the last
        union 
        (select mi.*												-- 3. toshift part 2: add 1 manual industry on top of the sequence (mi.industry_preorder = 0)
         from ticker_manual_industries mi
         join ticker_symbols_for_shift sfs on sfs.symbol = mi.symbol)
     ),
     
     ticker_industries_nrmlzd as (
       select tiu.symbol,
            tiu.industry_id,
            tiu.sim/(1e-30 + max(tiu.sim)over(partition by tiu.industry_id)) as sim,
            tiu.industry_preorder
       from ticker_industries_union tiu
     )

select concat(tin.symbol, '_', tin.industry_id)::varchar                                            as id,
       tin.symbol,
       tin.industry_id,
       tin.sim                                                                                      as similarity,
       row_number() over (partition by tin.symbol order by tin.sim desc, tin.industry_preorder asc) as industry_order,
       now()::timestamp                                                                             as updated_at
from ticker_industries_nrmlzd tin
