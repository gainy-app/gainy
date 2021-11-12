CREATE OR REPLACE VIEW "app"."profile_collections" AS
WITH profile_collections AS (
    SELECT NULL :: integer AS profile_id,
           collections.id,
           collections.name,
           collections.description,
           collections.image_url,
           collections.enabled,
           collections.personalized,
           collections.size
    FROM public.collections
    WHERE ((collections.personalized) :: text = '0' :: text)
    UNION
    SELECT csp.profile_id,
           c.id,
           c.name,
           c.description,
           c.image_url,
           c.enabled,
           c.personalized,
           csp.size
    FROM (
          public.collections c
             JOIN app.personalized_collection_sizes csp ON ((c.id = csp.collection_id))
        )
    WHERE ((c.personalized) :: text = '1' :: text)
)
SELECT profile_collections.profile_id,
       profile_collections.id,
       profile_collections.name,
       profile_collections.description,
       profile_collections.image_url,
       CASE
           WHEN ((profile_collections.enabled) :: text = '0' :: text) THEN '0' :: text
           WHEN ((profile_collections.size IS NULL) OR (profile_collections.size < 2)) THEN '0' :: text
           ELSE '1' :: text
           END                               AS enabled,
       profile_collections.personalized,
       COALESCE(profile_collections.size, 0) AS size
FROM profile_collections;