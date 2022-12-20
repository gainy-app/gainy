{{
  config(
    materialized = "view",
  )
}}

select *
from {{ source('app', 'profile_collection_match_score') }}
