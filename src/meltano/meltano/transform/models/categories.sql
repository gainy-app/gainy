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

WITH gainy_categories_with_collection_id AS (
    SELECT id::int,
           name::text,
           icon_url::text,
           risk_score::int,
           (10000 + id::int) as collection_id
    from {{ source ('gainy', 'gainy_categories') }}
    where enabled = '1'
      and _sdc_extracted_at > (select max(_sdc_extracted_at) from {{ source ('gainy', 'gainy_categories') }}) - interval '1 minute'
)
SELECT gc.id,
       gc.name,
       gc.icon_url,
       gc.risk_score,
       c.id as collection_id,
       now() as updated_at
FROM gainy_categories_with_collection_id gc
    -- The below reference to `collections` table is required for DBT to build correct model dependency graph
    LEFT JOIN {{ ref('collections') }} c
        ON gc.collection_id = c.id