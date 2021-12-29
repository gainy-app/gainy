{{
  config(
    materialized = "table",
    dist = "symbol",
    post_hook=[
      index(this, 'id', true),
      fk(this, 'collection_id', this.schema, 'collections', 'id')
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
)
SELECT gc.id, gc.name, gc.icon_url, gc.risk_score, gc.collection_id
FROM gainy_categories_with_collection_id gc
    -- The below reference to `collections` table is required for DBT to build correct model dependency graph
    JOIN {{ ref('collections') }} c
        ON gc.collection_id = c.id