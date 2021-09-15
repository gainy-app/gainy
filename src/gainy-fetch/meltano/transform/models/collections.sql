{{
  config(
    materialized = "table",
    sort = "created_at",
    post_hook=[
      index(this, 'id', true),
    ]
  )
}}

SELECT id::int, name, qa, description, enabled, image_url
FROM raw_collections