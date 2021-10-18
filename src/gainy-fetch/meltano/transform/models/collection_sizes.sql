{% set min_collection_size = 2 %}


{{
  config(
    materialized = "table",
    post_hook=[
      index(this, 'id', true),
    ]
  )
}}


select collection_id, count (*) as size
from {{ ref ('ticker_collections') }}
group by collection_id