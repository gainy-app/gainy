{% set min_collection_size = 2 %}


{{
  config(
    materialized = "table",
    post_hook=[
      index(this, 'id', true),
      fk('ticker_collections', 'collection_id', 'collections', 'id')
      fk('collection_sizes', 'collection_id', 'collections', 'id')
    ]
  )
}}


with raw_collections as (
    select id::int, name, description, enabled, personalized::int, image_url
    from {{ source ('gainy', 'raw_collections') }}
),
collection_sizes as (
    select * from {{ ref ('collection_sizes') }}
)
select c.id,
       c.name,
       c.description,
       case
           when c.personlized = then '1'
           when c.enabled = '0' then '0'
           when cs.size is null or cs.size < {{ min_collection_size }} then '0'
           else '1'
       end::varchar as enabled,
       c.personalized,
       c.image_url
from raw_collections c
         left join collection_sizes cs
                   on c.id = cs.collection_id