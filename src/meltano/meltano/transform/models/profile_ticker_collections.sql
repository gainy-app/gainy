{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}

SELECT ptc.profile_id,
       ptc.collection_id,
       (profile_id || '_' || collection_id) as collection_uniq_id,
       ptc.symbol
FROM {{ source('app', 'personalized_ticker_collections') }} ptc
JOIN {{ ref('collections') }} ON collections.id = ptc.collection_id
WHERE collections.enabled = '1'

UNION

SELECT NULL :: integer AS profile_id,
       tc.collection_id,
       ('0_' || collection_id) as collection_uniq_id,
       tc.symbol
FROM {{ ref('ticker_collections') }} tc
