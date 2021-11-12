{{
  config(
    materialized = "incremental",
    unique_key = "id",
    incremental_strategy='insert_overwrite',
    post_hook=[
      index(this, 'id', true),
    ]
  )
}}


with collections as (
    select id::int, name, description, enabled, personalized, image_url
    from {{ source ('gainy', 'gainy_collections') }}
)
select c.id,
       c.name,
       c.description,
       c.enabled,
       c.personalized,
       c.image_url,
       0::integer as size
from collections c
