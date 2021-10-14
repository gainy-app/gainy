{{
  config(
    materialized = "table",
    post_hook=[
      index(this, 'id', true),
    ]
  )
}}

SELECT id::int, name, icon_url, enabled
FROM {{ source('gainy', 'raw_interests') }}