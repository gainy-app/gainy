{{
  config(
    materialized = "table",
    sort = "created_at",
    post_hook=[
      index(this, 'id', true),
      index(this, 'name', true),
    ]
  )
}}

SELECT id::int, name, (20000 + id::int) as collection_id
FROM {{ source('gainy', 'gainy_industries') }}