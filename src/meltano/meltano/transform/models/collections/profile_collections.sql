{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}

WITH profile_collections AS (
    SELECT NULL :: integer                   AS profile_id,
           collections.id,
           ('0_' || collections.id)::varchar AS uniq_id,
           collections.name,
           collections.description,
           collections.image_url,
           collections.enabled,
           collections.personalized,
           influencers.name as influencer_name,
           collections.size
    FROM {{ ref('collections') }}
             left join {{ source('app', 'influencers') }} on influencers.id = collections.influencer_id
    WHERE collections.personalized = '0'
    UNION
    SELECT csp.profile_id,
           c.id,
           (csp.profile_id || '_' || c.id)::varchar AS uniq_id,
           c.name,
           c.description,
           c.image_url,
           c.enabled,
           c.personalized,
           null as influencer_name,
           csp.size
    FROM (
             {{ ref('collections') }} c
             JOIN {{ source('app', 'personalized_collection_sizes') }} csp ON ((c.id = csp.collection_id))
        )
    WHERE c.personalized = '1'
)
SELECT uniq_id,
       profile_collections.profile_id,
       profile_collections.id,
       profile_collections.name,
       profile_collections.description,
       profile_collections.image_url,
       CASE
           WHEN ((profile_collections.enabled) :: text = '0' :: text) THEN '0' :: text
           WHEN ((profile_collections.size IS NULL) OR (profile_collections.size < {{ var('min_collection_size') }})) THEN '0' :: text
           ELSE '1' :: text
           END                               AS enabled,
       profile_collections.personalized,
       profile_collections.influencer_name,
       COALESCE(profile_collections.size, 0) AS size
FROM profile_collections
where profile_collections.enabled = '1' and profile_collections.size >= {{ var('min_collection_size') }}
