CREATE OR REPLACE VIEW "app"."profile_ticker_collections" AS
SELECT ptc.profile_id,
       ptc.collection_id,
       ptc.symbol
FROM app.personalized_ticker_collections ptc
UNION
SELECT NULL :: integer AS profile_id,
       tc.collection_id,
       tc.symbol
FROM public.ticker_collections tc;