{{
  config(
    materialized = "view",
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
       collection_id,
       ('0_' || collection_id) as collection_uniq_id,
       symbol
FROM {{ ref('collection_ticker_actual_weights') }}
