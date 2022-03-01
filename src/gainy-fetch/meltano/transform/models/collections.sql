{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'id', true),
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}


with
{% if is_incremental() %}
     old_collections as (select * from {{ this }}),
{% endif %}
     collections as (
         select id::int, name, description, enabled, personalized, image_url
         from {{ source('gainy', 'gainy_collections') }}
     )
select c.id,
       c.name,
       c.description,
       c.enabled,
       c.personalized,
       c.image_url,
{% if is_incremental() %}
       coalesce(old_collections.size, 0::integer) as size,
{% else %}
       0::integer as size,
{% endif %}
       now()::timestamp as updated_at
from collections c
{% if is_incremental() %}
         left join old_collections on old_collections.id = c.id
{% endif %}
