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

SELECT id::int,
       name,
       icon_url,
       enabled,
       now()::timestamp as updated_at
FROM {{ source('gainy', 'gainy_interests') }}
where _sdc_extracted_at > (select max(_sdc_extracted_at) from {{ source('gainy', 'gainy_interests') }}) - interval '1 minute'
