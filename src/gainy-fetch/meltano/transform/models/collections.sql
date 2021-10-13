{{
  config(
    materialized = "table",
    post_hook=[
      index(this, 'id', true),
    ]
  )
}}

SELECT id::int, name, qa, description, enabled, image_url
FROM {{ source('gainy', 'raw_collections') }}