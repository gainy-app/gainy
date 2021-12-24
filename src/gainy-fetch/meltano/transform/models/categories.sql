{{
  config(
    materialized = "table",
    dist = "symbol",
    post_hook=[
      index(this, 'id', true),
    ]
  )
}}

SELECT id::int,
       name::text,
       icon_url::text,
       risk_score::int,
       (10000 + id::int) as collection_id
from {{ source ('gainy', 'gainy_categories') }}
where enabled = '1'
