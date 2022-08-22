{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}

select profile_collection_match_score.*,
       profile_collections.id as collection_id
from {{ source('app', 'profile_collection_match_score') }}
         join {{ ref('profile_collections') }} on profile_collections.uniq_id = collection_uniq_id
where profile_collections.enabled = '1'
