{% set min_collection_size = 2 %}


{{
  config(
    materialized = "table",
    post_hook=[
      index(this, 'id', true),
      fk('ticker_collections', 'collection_id', 'collections', 'id')
    ]
  )
}}


with collections as (
    select id::int, name, description, enabled, image_url
    from {{ source ('gainy', 'raw_collections') }}
),
base_collection_size as (
    select collection_id, count (*) as collection_size
    from {{ ref ('ticker_collections') }}
    group by collection_id
)
select c.id,
       c.name,
       c.description,
       case
           when c.enabled = '0' then '0'
           when cs.collection_size is null or cs.collection_size < {{ min_collection_size }} then '0'
           else '1'
        end::varchar as enabled,
       c.image_url,
       coalesce(cs.collection_size, 0)::integer as size
from collections c
    left join base_collection_size cs
        on c.id = cs.collection_id