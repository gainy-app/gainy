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

     ticker_auto_industries
         as ( -- union stack for the future: in case we would like more softeness on interests/industries we can use more top industries from the the model here
         (select ati.symbol            as symbol,
                 ati.industry_id_1     as industry_id,
                 ati.industry_1_cossim as sim,
                 1::int                as industry_preorder
          from {{ source('gainy', 'auto_ticker_industries') }} ati)
         union
         (select ati.symbol            as symbol,
                 ati.industry_id_2     as industry_id,
                 ati.industry_2_cossim as sim,
                 2::int                as industry_preorder
          from {{ source('gainy', 'auto_ticker_industries') }} ati)
     ),

     ticker_manual_industries as (
         select rgti.code  as symbol,
                gi.id::int as industry_id,
                1.1::float  as sim,              -- we completely sure about manual marked industries, sim=1.1
                0::int     as industry_preorder -- sim theoretically could be equal, so lets presave logical order to sort final industry order by this field
         from {{ source('gainy', 'gainy_ticker_industries') }} rgti -- 1 row
                  join {{ ref('gainy_industries') }} gi on gi."name" = rgti."industry name"
     ),

-- step 1. overwrite similarity=1 and preorder=0 if manual industry is in auto industries of the ticker (making that manual one as top 1 industry)
     ticker_industries_mi_over_ai as (
         select ai.symbol,
                ai.industry_id,
                coalesce(mi.sim, ai.sim)                             as sim,
                coalesce(mi.industry_preorder, ai.industry_preorder) as industry_preorder
         from ticker_auto_industries ai
                  left join ticker_manual_industries mi on mi.symbol = ai.symbol
             and mi.industry_id = ai.industry_id
     ),

-- step 2. after step 1, for all tickers where manual industry is defined and it still not in industries list:
--         we need to shift down industries and put manual on top of the sequence
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
         (select miovai.*
          from ticker_industries_mi_over_ai miovai -- 1. plain auto and manual overwrited auto excluding toshift part
                   left join ticker_symbols_for_shift sfs on sfs.symbol = miovai.symbol
          where sfs.symbol is null)
         union
         (select miovai.*
          from ticker_industries_mi_over_ai miovai -- 2. toshift part 1: drop the last industry (industry_preorder < max(industry_preorder) (that "max" gives the actual length of the industries sequence that model saves to ticker_auto_industries, so we are able to set the model to output and save more lenghty industries sequences, to be able to get more softeness on interests/industries match component))
                   join ticker_symbols_for_shift sfs on sfs.symbol = miovai.symbol
          where miovai.industry_preorder < (select max(industry_preorder) from ticker_auto_industries))
         -- if model somehow gives not length-equal sequences then we save here, coz for low-length sequences the manual industry will be just added without dropping the last
         union
         (select mi.* -- 3. toshift part 2: add 1 manual industry on top of the sequence (mi.industry_preorder = 0)
          from ticker_manual_industries mi
                   join ticker_symbols_for_shift sfs on sfs.symbol = mi.symbol)
     ),

     ticker_maxmin_sim
         as ( -- model classification generalization for each ticker: top 1st industry is max class for ticker, set is as 1.0 (all other classes for ticker must be normalized by /(max-min)=>E(f):(0..1.))
         select tiu.symbol,
                max(tiu.sim)   as sim_max, -- manual sim overwrites model's max sim it's ok
                ati.min_cossim as sim_min
         from ticker_industries_union tiu
                  join {{ source('gainy', 'auto_ticker_industries') }} ati on ati.symbol = tiu.symbol
         group by tiu.symbol, ati.min_cossim
     )

select concat(tiu.symbol, '_', industry_id)::varchar                                  as id,
       tiu.symbol,
       tiu.industry_id,
       (1e-30 + tiu.sim - tmms.sim_min) / (1e-30 + tmms.sim_max - tmms.sim_min)               as similarity, -- 1e-30 epsilon in both numerator and denominator
       row_number() over (partition by tiu.symbol order by tiu.industry_preorder asc) as industry_order,
       now()                                                                          as updated_at
from ticker_industries_union tiu
         join ticker_maxmin_sim tmms on tmms.symbol = tiu.symbol
         join common_stocks cs on cs.symbol = tiu.symbol
