{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('id'),
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}


with ticker_collections_weights as
         (

             select *
             from {{ source('gainy', 'ticker_collections_weights')}}
             where _sdc_extracted_at > (
                                           select max(_sdc_extracted_at)
                                           from {{ source('gainy', 'ticker_collections_weights')}}
                                       ) - interval '1 hour'
         ),
     ticker_collections as
         (

             select *
             from {{ source('gainy', 'ticker_collections')}}
             where _sdc_extracted_at > (
                                           select max(_sdc_extracted_at)
                                           from {{ source('gainy', 'ticker_collections')}}
                                       ) - interval '1 hour'
     ),
     ticker_sizes as materialized
         (
             (
                 with ticker_collections_weights_max_date as
                          (
                              select ttf_name,
                                     max(date) as date
                              from ticker_collections_weights
                              group by ttf_name
                          )
                 select ttf_name,
                        count(symbol) as size
                 from ticker_collections_weights_max_date
                          join ticker_collections_weights using (ttf_name, date)
                          join {{ ref('tickers') }} using (symbol)
                 group by ttf_name
             )

             union all

             select ttf_name, count(symbol) as size
             from ticker_collections
                      join {{ ref('tickers') }} using (symbol)
             group by ttf_name
     )
select c.id::int,
       c.name,
       c.description,
       c.enabled,
       c.personalized,
       c.image_url,
       c.influencer_id::int,
       ticker_sizes.size::int,
       now()::timestamp as updated_at
from {{ source('gainy', 'gainy_collections') }} c
         join ticker_sizes on ticker_sizes.ttf_name = c.name
where c._sdc_extracted_at > (select max(_sdc_extracted_at) from {{ source ('gainy', 'gainy_collections') }}) - interval '1 minute'
