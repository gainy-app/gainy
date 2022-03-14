{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'id', true),
      index(this, 'name', true),
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}

WITH gainy_industries_with_collection_id AS (
    SELECT id::int, name::character varying, (20000 + id::int) as collection_id
    FROM {{ source('gainy', 'gainy_industries') }}
)
SELECT gi.id,
       gi.name,
       c.id as collection_id,
       now()::timestamp as updated_at
FROM gainy_industries_with_collection_id gi
    -- The below reference to `collections` table is required for DBT to build correct model dependency graph
    LEFT JOIN {{ ref('collections') }} c
        ON gi.collection_id = c.id