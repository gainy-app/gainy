{{
  config(
    materialized = "table",
    sort = "created_at",
    post_hook=[
      index(this, 'id', true),
      index(this, 'name', true),
      fk(this, 'collection_id', this.schema, 'collections', 'id')
    ]
  )
}}

WITH gainy_industries_with_collection_id AS (
    SELECT id::int, name::character varying, (20000 + id::int) as collection_id
    FROM {{ source('gainy', 'gainy_industries') }}
)
SELECT gi.id, gi.name, gi.collection_id
FROM gainy_industries_with_collection_id gi
    -- The below reference to `collections` table is required for DBT to build correct model dependency graph
    JOIN {{ ref('collections') }} c
        ON gi.collection_id = c.id