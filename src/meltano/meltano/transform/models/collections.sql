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


select c.id::int,
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
from {{ source('gainy', 'gainy_collections') }} c
{% if is_incremental() %}
         left join {{ this }} old_collections on old_collections.id = c.id::int
{% endif %}
where c._sdc_extracted_at > (select max(_sdc_extracted_at) from {{ source ('gainy', 'gainy_collections') }}) - interval '1 minute'
