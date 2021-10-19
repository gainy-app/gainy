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
    select id::int, name, description, enabled, personalized, image_url
    from {{ source ('gainy', 'raw_collections') }}
),
collection_sizes as (
    select collection_id, count (*) as collection_size
    from {{ ref ('ticker_collections') }}
    group by collection_id
)
select c.id,
       c.name,
       c.description,
       c.enabled,
       c.personalized,
       c.image_url,
       cs.collection_size::integer as size
from collections c
    left join collection_sizes cs
on c.id = cs.collection_id