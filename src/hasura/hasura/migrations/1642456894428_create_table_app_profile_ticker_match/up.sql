create table app.profile_ticker_match_score_json (
    profile                int4  PRIMARY KEY,
    match_score_json       jsonb NOT NULL
);


create view app.profile_ticker_match_score as
with tmp_exploded_match_score as (
    select
        profile,
        match_score
    from
        app.profile_ticker_match_score_json, jsonb_array_elements(app.profile_ticker_match_score_json.match_score_json) as match_score
)
select
    profile as profile_id,
    (match_score ->> 'symbol')::text as symbol,
        (match_score -> 'match_score')::int4 as match_score,
        (match_score -> 'fits_risk')::int4 as fits_risk,
        (match_score -> 'risk_similarity')::float as risk_similarity,
        (match_score -> 'fits_categories')::int4 as fits_categories,
        ARRAY(SELECT jsonb_array_elements(match_score -> 'category_matches'))::int4[] as category_matches,
        (match_score -> 'fits_interests')::int4 as fits_interests,
        ARRAY(SELECT jsonb_array_elements(match_score -> 'interest_matches'))::int4[] as interest_matches
from
    tmp_exploded_match_score;