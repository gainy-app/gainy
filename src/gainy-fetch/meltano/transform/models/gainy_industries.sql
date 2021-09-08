{{
  config(
    materialized = "table",
    sort = "created_at",
    post_hook=[
      index(this, 'id', true),
    ]
  )
}}

SELECT id::int, name
FROM raw_gainy_industries