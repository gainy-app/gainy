{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}

SELECT ptc.profile_id,
       ptc.collection_id,
       ptc.symbol
FROM {{ source('app', 'personalized_ticker_collections') }} ptc
UNION
SELECT NULL :: integer AS profile_id,
       tc.collection_id,
       tc.symbol
FROM {{ ref('ticker_collections') }} tc
